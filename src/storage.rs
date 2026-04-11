use std::cmp::Ordering;
use std::fs::{File, OpenOptions};
use std::io::{BufReader, BufWriter, Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};

use anyhow::{bail, ensure, Context, Result};
use lz4_flex::block;
use memmap2::{Mmap, MmapOptions};
use once_cell::sync::OnceCell;
use ordered_float::OrderedFloat;
use rustc_hash::FxHashMap;

use crate::types::{BatchColumn, ColumnDef, ColumnarBatch, DataType, Schema, Value};

const MAGIC: &[u8; 8] = b"NRWDB007";
const RECORD_CREATE_TABLE: u8 = 1;
const RECORD_ROW_GROUP: u8 = 2;
const ROW_GROUP_COMPRESSION_RAW: u8 = 0;
const ROW_GROUP_COMPRESSION_LZ4: u8 = 1;
const INT_ENCODING_RAW: u8 = 0;
const INT_ENCODING_BASE_U8: u8 = 1;
const INT_ENCODING_BASE_U16: u8 = 2;
const INT_ENCODING_BASE_U32: u8 = 3;
const STRING_ENCODING_PLAIN: u8 = 0;
const STRING_ENCODING_DICT: u8 = 1;
const CODE_ENCODING_U8: u8 = 0;
const CODE_ENCODING_U16: u8 = 1;
const CODE_ENCODING_U32: u8 = 2;

use std::time::Duration;

#[derive(Debug, Clone)]
pub struct DbOptions {
    pub row_group_size: usize,
    pub sync_on_flush: bool,
    pub auto_flush_interval: Option<Duration>,
}

impl Default for DbOptions {
    fn default() -> Self {
        Self {
            row_group_size: 16_384,
            sync_on_flush: true,
            auto_flush_interval: None,
        }
    }
}

// ---------------------------------------------------------------------------
// Null bitmap: packed bit-vector where set bit = value present, clear bit = null
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NullBitmap {
    data: Vec<u8>,
    len: usize,
}

impl NullBitmap {
    #[allow(dead_code)]
    pub fn len(&self) -> usize {
        self.len
    }

    #[allow(dead_code)]
    pub fn is_empty(&self) -> bool {
        self.len == 0
    }

    pub fn data(&self) -> &[u8] {
        &self.data
    }

    pub fn is_null(&self, index: usize) -> bool {
        !self.is_present(index)
    }

    pub fn is_present(&self, index: usize) -> bool {
        debug_assert!(index < self.len);
        (self.data[index / 8] >> (index % 8)) & 1 != 0
    }

    pub fn from_bools(present: &[bool]) -> Self {
        let len = present.len();
        let byte_len = (len + 7) / 8;
        let mut data = vec![0u8; byte_len];
        for (i, &p) in present.iter().enumerate() {
            if p {
                data[i / 8] |= 1 << (i % 8);
            }
        }
        Self { data, len }
    }

    pub fn null_count(&self) -> usize {
        (0..self.len).filter(|&i| self.is_null(i)).count()
    }

    fn encode(&self) -> Vec<u8> {
        self.data.clone()
    }

    fn decode(data: Vec<u8>, len: usize) -> Self {
        Self { data, len }
    }
}

#[derive(Debug, Clone)]
pub struct ColumnStats {
    pub min: Option<Value>,
    pub max: Option<Value>,
    pub null_count: usize,
}

impl ColumnStats {
    fn from_column(column: &ColumnData, nulls: Option<&NullBitmap>) -> Self {
        let (min, max) = match column {
            ColumnData::Int64(values) => {
                let (min, max) = min_max_with_nulls(values, nulls, |v| *v);
                (min.map(Value::Int64), max.map(Value::Int64))
            }
            ColumnData::Float64(values) => {
                let (min, max) = min_max_with_nulls(values, nulls, |v| *v);
                (min.map(Value::Float64), max.map(Value::Float64))
            }
            ColumnData::Bool(values) => {
                let (min, max) = min_max_with_nulls(values, nulls, |v| *v);
                (min.map(Value::Bool), max.map(Value::Bool))
            }
            ColumnData::StringPlain(values) => {
                let (min, max) = min_max_with_nulls(values, nulls, |v| v.clone());
                (min.map(Value::String), max.map(Value::String))
            }
            ColumnData::StringDict { dictionary, codes } => {
                let mut min = None;
                let mut max = None;
                for (i, code) in codes.iter().enumerate() {
                    if nulls.is_some_and(|n| n.is_null(i)) {
                        continue;
                    }
                    let value = &dictionary[*code as usize];
                    if min.as_ref().is_none_or(|current: &String| value < current) {
                        min = Some(value.clone());
                    }
                    if max.as_ref().is_none_or(|current: &String| value > current) {
                        max = Some(value.clone());
                    }
                }
                (min.map(Value::String), max.map(Value::String))
            }
        };
        let null_count = nulls.map(|n| n.null_count()).unwrap_or(0);
        Self {
            min,
            max,
            null_count,
        }
    }
}

fn min_max_with_nulls<T, U: Ord + Clone>(
    values: &[T],
    nulls: Option<&NullBitmap>,
    f: impl Fn(&T) -> U,
) -> (Option<U>, Option<U>) {
    let mut min = None;
    let mut max = None;
    for (i, v) in values.iter().enumerate() {
        if nulls.is_some_and(|n| n.is_null(i)) {
            continue;
        }
        let key = f(v);
        if min.as_ref().is_none_or(|m| &key < m) {
            min = Some(key.clone());
        }
        if max.as_ref().is_none_or(|m| &key > m) {
            max = Some(key);
        }
    }
    (min, max)
}

#[derive(Debug, Clone)]
pub enum ColumnData {
    Int64(Vec<i64>),
    Float64(Vec<OrderedFloat<f64>>),
    Bool(Vec<bool>),
    StringPlain(Vec<String>),
    StringDict {
        dictionary: Vec<String>,
        codes: Vec<u32>,
    },
}

fn build_string_column(values: Vec<String>) -> ColumnData {
    if values.is_empty() {
        return ColumnData::StringPlain(values);
    }

    let max_dictionary_size = values.len() / 2;
    let mut lookup: FxHashMap<&str, u32> = FxHashMap::default();
    let mut dictionary_refs = Vec::new();

    for value in &values {
        if lookup.contains_key(value.as_str()) {
            continue;
        }
        if dictionary_refs.len() > max_dictionary_size {
            return ColumnData::StringPlain(values);
        }
        let code = dictionary_refs.len() as u32;
        lookup.insert(value.as_str(), code);
        dictionary_refs.push(value.as_str());
    }

    let dictionary = dictionary_refs.into_iter().map(str::to_owned).collect();
    let codes = values.iter().map(|value| lookup[value.as_str()]).collect();

    ColumnData::StringDict { dictionary, codes }
}

impl ColumnData {
    pub fn len(&self) -> usize {
        match self {
            Self::Int64(values) => values.len(),
            Self::Float64(values) => values.len(),
            Self::Bool(values) => values.len(),
            Self::StringPlain(values) => values.len(),
            Self::StringDict { codes, .. } => codes.len(),
        }
    }

    pub fn value_at(&self, row: usize) -> Value {
        match self {
            Self::Int64(values) => Value::Int64(values[row]),
            Self::Float64(values) => Value::Float64(values[row]),
            Self::Bool(values) => Value::Bool(values[row]),
            Self::StringPlain(values) => Value::String(values[row].clone()),
            Self::StringDict { dictionary, codes } => {
                Value::String(dictionary[codes[row] as usize].clone())
            }
        }
    }

    pub fn compare_at(&self, row: usize, value: &Value) -> Option<Ordering> {
        match (self, value) {
            (Self::Int64(values), Value::Int64(target)) => Some(values[row].cmp(target)),
            (Self::Int64(values), Value::Float64(target)) => {
                Some(OrderedFloat(values[row] as f64).cmp(target))
            }
            (Self::Float64(values), Value::Float64(target)) => Some(values[row].cmp(target)),
            (Self::Float64(values), Value::Int64(target)) => {
                Some(values[row].cmp(&OrderedFloat(*target as f64)))
            }
            (Self::Bool(values), Value::Bool(target)) => Some(values[row].cmp(target)),
            (Self::StringPlain(values), Value::String(target)) => {
                Some(values[row].as_str().cmp(target.as_str()))
            }
            (Self::StringDict { dictionary, codes }, Value::String(target)) => Some(
                dictionary[codes[row] as usize]
                    .as_str()
                    .cmp(target.as_str()),
            ),
            _ => None,
        }
    }

    pub fn numeric_at(&self, row: usize) -> Option<f64> {
        match self {
            Self::Int64(values) => Some(values[row] as f64),
            Self::Float64(values) => Some(values[row].into_inner()),
            _ => None,
        }
    }
}

#[derive(Debug, Clone)]
pub struct RowGroup {
    pub rows: usize,
    pub columns: Vec<ColumnData>,
    pub nulls: Vec<Option<NullBitmap>>,
    pub stats: Vec<ColumnStats>,
}

impl RowGroup {
    fn new(columns: Vec<ColumnData>, nulls: Vec<Option<NullBitmap>>) -> Self {
        let rows = columns.first().map(ColumnData::len).unwrap_or(0);
        let stats = columns
            .iter()
            .zip(nulls.iter())
            .map(|(col, n)| ColumnStats::from_column(col, n.as_ref()))
            .collect();
        Self {
            rows,
            columns,
            nulls,
            stats,
        }
    }

    #[allow(dead_code)]
    pub fn is_null(&self, column_index: usize, row: usize) -> bool {
        self.nulls[column_index]
            .as_ref()
            .is_some_and(|n| n.is_null(row))
    }
}

#[derive(Debug)]
pub struct LoadedRowGroup<'a> {
    pub rows: usize,
    pub stats: &'a [ColumnStats],
    columns: Vec<Option<&'a ColumnData>>,
    nulls: Vec<Option<&'a NullBitmap>>,
}

impl<'a> LoadedRowGroup<'a> {
    fn from_row_group(row_group: &'a RowGroup, required_columns: &[usize]) -> Self {
        let mut columns = vec![None; row_group.columns.len()];
        let mut nulls = vec![None; row_group.nulls.len()];
        for &index in required_columns {
            columns[index] = Some(&row_group.columns[index]);
            nulls[index] = row_group.nulls[index].as_ref();
        }
        Self {
            rows: row_group.rows,
            stats: &row_group.stats,
            columns,
            nulls,
        }
    }

    pub fn column(&self, index: usize) -> &ColumnData {
        self.columns[index].expect("requested column was not loaded")
    }

    pub fn nulls(&self, index: usize) -> Option<&NullBitmap> {
        self.nulls[index]
    }
}

#[derive(Debug)]
pub struct StoredRowGroup {
    rows: usize,
    stats: Vec<ColumnStats>,
    in_memory: Option<RowGroup>,
    source: Option<StoredRowGroupSource>,
}

#[derive(Debug)]
struct StoredRowGroupSource {
    columns: Vec<StoredColumnSource>,
}

#[derive(Debug)]
struct StoredColumnSource {
    nulls: Option<StoredBlob>,
    data: StoredBlob,
    decoded_column: OnceCell<ColumnData>,
    decoded_nulls: OnceCell<Option<NullBitmap>>,
}

#[derive(Debug, Clone, Copy)]
struct StoredBlob {
    offset: usize,
    stored_len: usize,
    raw_len: usize,
    compression: u8,
}

impl StoredRowGroup {
    pub fn from_row_group(row_group: RowGroup) -> Self {
        Self {
            rows: row_group.rows,
            stats: row_group.stats.clone(),
            in_memory: Some(row_group),
            source: None,
        }
    }

    fn from_mapped(rows: usize, stats: Vec<ColumnStats>, columns: Vec<StoredColumnSource>) -> Self {
        Self {
            rows,
            stats,
            in_memory: None,
            source: Some(StoredRowGroupSource { columns }),
        }
    }

    pub fn rows(&self) -> usize {
        self.rows
    }

    pub fn stats(&self) -> &[ColumnStats] {
        &self.stats
    }

    pub fn load<'a>(
        &'a self,
        schema: &Schema,
        mapped: Option<&'a [u8]>,
        required_columns: &[usize],
    ) -> Result<LoadedRowGroup<'a>> {
        if let Some(row_group) = &self.in_memory {
            return Ok(LoadedRowGroup::from_row_group(row_group, required_columns));
        }

        let source = self
            .source
            .as_ref()
            .context("lazy row group source missing")?;
        let mapped = mapped.context("mapped storage unavailable for lazy row group")?;
        let mut columns = vec![None; schema.columns.len()];
        let mut nulls = vec![None; schema.columns.len()];

        for &index in required_columns {
            let column_source = &source.columns[index];
            columns[index] = Some(column_source.decode_column(
                schema.columns[index].data_type,
                self.rows,
                mapped,
            )?);
            nulls[index] = column_source.decode_nulls(self.rows, mapped)?;
        }

        Ok(LoadedRowGroup {
            rows: self.rows,
            stats: &self.stats,
            columns,
            nulls,
        })
    }
}

impl StoredColumnSource {
    fn new(nulls: Option<StoredBlob>, data: StoredBlob) -> Self {
        Self {
            nulls,
            data,
            decoded_column: OnceCell::new(),
            decoded_nulls: OnceCell::new(),
        }
    }

    fn decode_column<'a>(
        &'a self,
        data_type: DataType,
        rows: usize,
        mapped: &'a [u8],
    ) -> Result<&'a ColumnData> {
        self.decoded_column.get_or_try_init(|| {
            let bytes = self.data.read(mapped)?;
            decode_column_data(&bytes, data_type, rows)
        })
    }

    fn decode_nulls<'a>(&'a self, rows: usize, mapped: &'a [u8]) -> Result<Option<&'a NullBitmap>> {
        let Some(blob) = self.nulls else {
            return Ok(None);
        };
        let bitmap = self.decoded_nulls.get_or_try_init(|| {
            let bytes = blob.read(mapped)?;
            Ok::<Option<NullBitmap>, anyhow::Error>(Some(NullBitmap::decode(bytes, rows)))
        })?;
        Ok(bitmap.as_ref())
    }
}

impl StoredBlob {
    fn read(self, mapped: &[u8]) -> Result<Vec<u8>> {
        let bytes = mapped
            .get(self.offset..self.offset + self.stored_len)
            .context("stored blob out of bounds")?;
        match self.compression {
            ROW_GROUP_COMPRESSION_RAW => Ok(bytes.to_vec()),
            ROW_GROUP_COMPRESSION_LZ4 => {
                block::decompress(bytes, self.raw_len).context("failed to decompress stored blob")
            }
            other => bail!("unknown stored blob compression: {other}"),
        }
    }
}

#[derive(Debug, Clone)]
pub enum ColumnBuilder {
    Int64(Vec<i64>, Vec<bool>),
    Float64(Vec<OrderedFloat<f64>>, Vec<bool>),
    Bool(Vec<bool>, Vec<bool>),
    String(Vec<String>, Vec<bool>),
}

impl ColumnBuilder {
    pub fn with_capacity(data_type: DataType, capacity: usize) -> Self {
        match data_type {
            DataType::Int64 | DataType::Timestamp => {
                Self::Int64(Vec::with_capacity(capacity), Vec::with_capacity(capacity))
            }
            DataType::Float64 => {
                Self::Float64(Vec::with_capacity(capacity), Vec::with_capacity(capacity))
            }
            DataType::Bool => {
                Self::Bool(Vec::with_capacity(capacity), Vec::with_capacity(capacity))
            }
            DataType::String => {
                Self::String(Vec::with_capacity(capacity), Vec::with_capacity(capacity))
            }
        }
    }

    pub fn append(&mut self, value: Value) -> Result<()> {
        match (self, value) {
            (Self::Int64(values, nulls), Value::Int64(value)) => {
                values.push(value);
                nulls.push(true);
            }
            (Self::Int64(values, nulls), Value::Null) => {
                values.push(0);
                nulls.push(false);
            }
            (Self::Float64(values, nulls), Value::Float64(value)) => {
                values.push(value);
                nulls.push(true);
            }
            (Self::Float64(values, nulls), Value::Null) => {
                values.push(OrderedFloat(0.0));
                nulls.push(false);
            }
            (Self::Bool(values, nulls), Value::Bool(value)) => {
                values.push(value);
                nulls.push(true);
            }
            (Self::Bool(values, nulls), Value::Null) => {
                values.push(false);
                nulls.push(false);
            }
            (Self::String(values, nulls), Value::String(value)) => {
                values.push(value);
                nulls.push(true);
            }
            (Self::String(values, nulls), Value::Null) => {
                values.push(String::new());
                nulls.push(false);
            }
            (builder, value) => bail!("cannot append {value:?} into {builder:?}"),
        }
        Ok(())
    }

    pub fn len(&self) -> usize {
        match self {
            Self::Int64(values, _) => values.len(),
            Self::Float64(values, _) => values.len(),
            Self::Bool(values, _) => values.len(),
            Self::String(values, _) => values.len(),
        }
    }

    pub fn finish(&mut self) -> (ColumnData, Option<NullBitmap>) {
        match self {
            Self::Int64(values, nulls) => {
                let data = ColumnData::Int64(std::mem::take(values));
                let bitmap = if nulls.iter().all(|&p| p) {
                    None
                } else {
                    Some(NullBitmap::from_bools(nulls))
                };
                (data, bitmap)
            }
            Self::Float64(values, nulls) => {
                let data = ColumnData::Float64(std::mem::take(values));
                let bitmap = if nulls.iter().all(|&p| p) {
                    None
                } else {
                    Some(NullBitmap::from_bools(nulls))
                };
                (data, bitmap)
            }
            Self::Bool(values, nulls) => {
                let data = ColumnData::Bool(std::mem::take(values));
                let bitmap = if nulls.iter().all(|&p| p) {
                    None
                } else {
                    Some(NullBitmap::from_bools(nulls))
                };
                (data, bitmap)
            }
            Self::String(values, nulls) => {
                let data = build_string_column(std::mem::take(values));
                let bitmap = if nulls.iter().all(|&p| p) {
                    None
                } else {
                    // For StringDict, the null bitmap refers to the original row positions
                    // (before dictionary encoding), so it's still valid.
                    Some(NullBitmap::from_bools(nulls))
                };
                (data, bitmap)
            }
        }
    }

    pub fn append_batch_column(&mut self, column: BatchColumn) -> Result<()> {
        match (self, column) {
            (Self::Int64(values, nulls), BatchColumn::Int64(mut batch))
            | (Self::Int64(values, nulls), BatchColumn::Timestamp(mut batch)) => {
                values.append(&mut batch);
                nulls.extend(std::iter::repeat_n(true, batch.len()));
            }
            (Self::Float64(values, nulls), BatchColumn::Float64(batch)) => {
                let len = batch.len();
                values.extend(batch.into_iter().map(OrderedFloat));
                nulls.extend(std::iter::repeat_n(true, len));
            }
            (Self::Bool(values, nulls), BatchColumn::Bool(mut batch)) => {
                let len = batch.len();
                values.append(&mut batch);
                nulls.extend(std::iter::repeat_n(true, len));
            }
            (Self::String(values, nulls), BatchColumn::String(mut batch)) => {
                let len = batch.len();
                values.append(&mut batch);
                nulls.extend(std::iter::repeat_n(true, len));
            }
            (builder, batch) => bail!("cannot append batch column {batch:?} into {builder:?}"),
        }
        Ok(())
    }
}

#[derive(Debug, Clone)]
pub struct PendingBatch {
    pub columns: Vec<ColumnBuilder>,
}

impl PendingBatch {
    pub(crate) fn new(schema: &Schema, capacity: usize) -> Self {
        Self {
            columns: schema
                .columns
                .iter()
                .map(|column| ColumnBuilder::with_capacity(column.data_type, capacity))
                .collect(),
        }
    }

    pub fn rows(&self) -> usize {
        self.columns.first().map(ColumnBuilder::len).unwrap_or(0)
    }

    pub fn append_row(&mut self, row: Vec<Value>) -> Result<()> {
        ensure!(row.len() == self.columns.len(), "row width mismatch");
        for (builder, value) in self.columns.iter_mut().zip(row) {
            builder.append(value)?;
        }
        Ok(())
    }

    pub fn take_row_group(&mut self) -> RowGroup {
        let (columns, nulls): (Vec<_>, Vec<_>) =
            self.columns.iter_mut().map(ColumnBuilder::finish).unzip();
        RowGroup::new(columns, nulls)
    }

    pub fn append_columnar_batch(&mut self, batch: ColumnarBatch) -> Result<()> {
        ensure!(
            batch.columns().len() == self.columns.len(),
            "batch width mismatch"
        );
        for (builder, column) in self.columns.iter_mut().zip(batch.into_columns()) {
            builder.append_batch_column(column)?;
        }
        Ok(())
    }
}

#[derive(Debug)]
pub struct Table {
    pub schema: Schema,
    pub row_groups: Vec<StoredRowGroup>,
    pub pending: PendingBatch,
}

impl Table {
    pub(crate) fn new(schema: Schema, options: &DbOptions) -> Self {
        let pending = PendingBatch::new(&schema, options.row_group_size);
        Self {
            schema,
            row_groups: Vec::new(),
            pending,
        }
    }
}

pub(crate) fn row_group_from_columnar_batch(batch: ColumnarBatch) -> Result<RowGroup> {
    let columns = batch
        .into_columns()
        .into_iter()
        .map(column_data_from_batch_column)
        .collect::<Result<Vec<_>>>()?;
    let nulls = vec![None; columns.len()];
    Ok(RowGroup::new(columns, nulls))
}

fn column_data_from_batch_column(column: BatchColumn) -> Result<ColumnData> {
    match column {
        BatchColumn::Int64(values) | BatchColumn::Timestamp(values) => {
            Ok(ColumnData::Int64(values))
        }
        BatchColumn::Float64(values) => Ok(ColumnData::Float64(
            values.into_iter().map(OrderedFloat).collect(),
        )),
        BatchColumn::Bool(values) => Ok(ColumnData::Bool(values)),
        BatchColumn::String(values) => Ok(build_string_column(values)),
    }
}

pub struct Storage {
    path: PathBuf,
    file: File,
    options: DbOptions,
    mmap: Option<Mmap>,
}

impl Storage {
    pub fn open(
        path: impl AsRef<Path>,
        options: DbOptions,
    ) -> Result<(Self, FxHashMap<String, Table>)> {
        let path = path.as_ref().to_path_buf();
        let mut file = OpenOptions::new()
            .create(true)
            .read(true)
            .write(true)
            .open(&path)
            .with_context(|| format!("opening database file {}", path.display()))?;

        if file.metadata()?.len() == 0 {
            file.write_all(MAGIC)?;
            file.sync_all()?;
        }

        file.seek(SeekFrom::Start(0))?;
        let mut header = [0_u8; 8];
        file.read_exact(&mut header)?;
        ensure!(
            &header == MAGIC,
            "invalid database header or unsupported format version"
        );

        let tables = Self::load_tables(&path, &options)?;
        let mmap = unsafe { Some(MmapOptions::new().map(&file)?) };
        file.seek(SeekFrom::End(0))?;

        Ok((
            Self {
                path,
                file,
                options,
                mmap,
            },
            tables,
        ))
    }

    pub fn options(&self) -> &DbOptions {
        &self.options
    }

    pub fn mapped_bytes(&self) -> Option<&[u8]> {
        self.mmap.as_deref()
    }

    pub fn remap(&mut self) -> Result<()> {
        self.mmap = Some(unsafe { MmapOptions::new().map(&self.file)? });
        Ok(())
    }

    pub fn append_create_table(&mut self, schema: &Schema) -> Result<()> {
        let payload = encode_schema(schema)?;
        self.append_record(RECORD_CREATE_TABLE, &payload)
    }

    pub fn append_row_group(
        &mut self,
        table_name: &str,
        row_group: &RowGroup,
        schema: &Schema,
    ) -> Result<()> {
        let payload = encode_row_group(table_name, row_group, schema)?;
        self.append_record(RECORD_ROW_GROUP, &payload)
    }

    fn append_record(&mut self, kind: u8, payload: &[u8]) -> Result<()> {
        let mut writer = BufWriter::new(&self.file);
        writer.write_all(&[kind])?;
        writer.write_all(&(payload.len() as u64).to_le_bytes())?;
        writer.write_all(payload)?;
        writer.flush()?;
        drop(writer);
        if self.options.sync_on_flush {
            self.file.sync_data()?;
        }
        Ok(())
    }

    fn load_tables(path: &Path, options: &DbOptions) -> Result<FxHashMap<String, Table>> {
        let file = OpenOptions::new().read(true).open(path)?;
        let mut reader = BufReader::new(file);
        let mut header = [0_u8; 8];
        reader.read_exact(&mut header)?;
        ensure!(
            &header == MAGIC,
            "invalid database header or unsupported format version"
        );

        let mut schemas = FxHashMap::default();
        let mut tables = FxHashMap::default();

        loop {
            let mut kind = [0_u8; 1];
            let bytes = reader.read(&mut kind)?;
            if bytes == 0 {
                break;
            }

            let mut len_buf = [0_u8; 8];
            reader.read_exact(&mut len_buf)?;
            let len = u64::from_le_bytes(len_buf) as usize;
            let payload_offset = reader.stream_position()? as usize;

            match kind[0] {
                RECORD_CREATE_TABLE => {
                    let mut payload = vec![0_u8; len];
                    reader.read_exact(&mut payload)?;
                    let schema = decode_schema(&payload)?;
                    let table = Table::new(schema.clone(), options);
                    schemas.insert(schema.table_name.clone(), schema.clone());
                    tables.insert(schema.table_name.clone(), table);
                }
                RECORD_ROW_GROUP => {
                    let table_name = read_string_from_reader(&mut reader)?;
                    let rows = read_u32_from_reader(&mut reader)? as usize;
                    let metadata_len = read_u32_from_reader(&mut reader)? as usize;
                    let mut metadata = vec![0_u8; metadata_len];
                    reader.read_exact(&mut metadata)?;
                    let data_base_offset = reader.stream_position()? as usize;
                    let table = tables
                        .get_mut(&table_name)
                        .with_context(|| format!("missing table for row group {table_name}"))?;
                    let (stats, columns) =
                        decode_row_group_source(&metadata, &table.schema, rows, data_base_offset)?;
                    table
                        .row_groups
                        .push(StoredRowGroup::from_mapped(rows, stats, columns));
                    reader.seek(SeekFrom::Start((payload_offset + len) as u64))?;
                }
                other => bail!("unknown record kind: {other}"),
            }
        }

        Ok(tables)
    }

    pub fn path(&self) -> &Path {
        &self.path
    }
}

fn encode_schema(schema: &Schema) -> Result<Vec<u8>> {
    let mut bytes = Vec::new();
    write_string(&mut bytes, &schema.table_name)?;
    write_u16(&mut bytes, schema.columns.len() as u16);
    for column in &schema.columns {
        write_string(&mut bytes, &column.name)?;
        bytes.push(column.data_type.tag());
    }
    Ok(bytes)
}

fn decode_schema(payload: &[u8]) -> Result<Schema> {
    let mut cursor = 0;
    let table_name = read_string(payload, &mut cursor)?;
    let count = read_u16(payload, &mut cursor)? as usize;
    let mut columns = Vec::with_capacity(count);
    for _ in 0..count {
        let name = read_string(payload, &mut cursor)?;
        let tag = read_u8(payload, &mut cursor)?;
        columns.push(ColumnDef {
            name,
            data_type: DataType::from_tag(tag)?,
        });
    }
    Ok(Schema {
        table_name,
        columns,
    })
}

fn encode_row_group(table_name: &str, row_group: &RowGroup, schema: &Schema) -> Result<Vec<u8>> {
    let mut bytes = Vec::new();
    write_string(&mut bytes, table_name)?;
    write_u32(&mut bytes, row_group.rows as u32);

    let mut metadata = Vec::new();
    let mut data_section = Vec::new();
    for (((column, null_bitmap), column_def), stats) in row_group
        .columns
        .iter()
        .zip(&row_group.nulls)
        .zip(&schema.columns)
        .zip(&row_group.stats)
    {
        encode_column_stats(&mut metadata, stats, column_def.data_type)?;

        let null_blob = null_bitmap.as_ref().map(|bitmap| {
            let offset = data_section.len();
            let encoded = bitmap.encode();
            data_section.extend_from_slice(&encoded);
            StoredBlob {
                offset,
                stored_len: encoded.len(),
                raw_len: encoded.len(),
                compression: ROW_GROUP_COMPRESSION_RAW,
            }
        });
        encode_optional_blob_descriptor(&mut metadata, null_blob)?;

        let mut body = Vec::new();

        match (column, column_def.data_type) {
            (ColumnData::Int64(values), DataType::Int64 | DataType::Timestamp) => {
                encode_int64_values(&mut body, values);
            }
            (ColumnData::Float64(values), DataType::Float64) => {
                for value in values {
                    body.extend_from_slice(&value.into_inner().to_le_bytes());
                }
            }
            (ColumnData::Bool(values), DataType::Bool) => {
                encode_packed_bools(&mut body, values.iter().copied());
            }
            (ColumnData::StringPlain(values), DataType::String) => {
                body.push(STRING_ENCODING_PLAIN);
                for value in values {
                    write_string(&mut body, value)?;
                }
            }
            (ColumnData::StringDict { dictionary, codes }, DataType::String) => {
                body.push(STRING_ENCODING_DICT);
                write_u32(&mut body, dictionary.len() as u32);
                for value in dictionary {
                    write_string(&mut body, value)?;
                }
                encode_code_values(&mut body, dictionary.len(), codes)?;
            }
            _ => bail!("column type mismatch while encoding row group"),
        }

        let data_blob = compress_blob(body);
        let stored_blob = StoredBlob {
            offset: data_section.len(),
            stored_len: data_blob.stored.len(),
            raw_len: data_blob.raw_len,
            compression: data_blob.compression,
        };
        data_section.extend_from_slice(&data_blob.stored);
        encode_blob_descriptor(&mut metadata, stored_blob);
    }

    write_u32(&mut bytes, metadata.len() as u32);
    bytes.extend_from_slice(&metadata);
    bytes.extend_from_slice(&data_section);
    Ok(bytes)
}

fn decode_column_data(payload: &[u8], data_type: DataType, rows: usize) -> Result<ColumnData> {
    let mut cursor = 0;
    let data = match data_type {
        DataType::Int64 | DataType::Timestamp => {
            ColumnData::Int64(decode_int64_values(payload, &mut cursor, rows)?)
        }
        DataType::Float64 => {
            let mut values = Vec::with_capacity(rows);
            for _ in 0..rows {
                values.push(OrderedFloat(read_f64(payload, &mut cursor)?));
            }
            ColumnData::Float64(values)
        }
        DataType::Bool => ColumnData::Bool(decode_packed_bools(payload, &mut cursor, rows)?),
        DataType::String => match read_u8(payload, &mut cursor)? {
            STRING_ENCODING_PLAIN => {
                let mut values = Vec::with_capacity(rows);
                for _ in 0..rows {
                    values.push(read_string(payload, &mut cursor)?);
                }
                ColumnData::StringPlain(values)
            }
            STRING_ENCODING_DICT => {
                let dictionary_len = read_u32(payload, &mut cursor)? as usize;
                let mut dictionary = Vec::with_capacity(dictionary_len);
                for _ in 0..dictionary_len {
                    dictionary.push(read_string(payload, &mut cursor)?);
                }
                let codes = decode_code_values(payload, &mut cursor, rows)?;
                ColumnData::StringDict { dictionary, codes }
            }
            other => bail!("unknown string column encoding: {other}"),
        },
    };
    ensure!(cursor == payload.len(), "column payload had trailing bytes");
    Ok(data)
}

struct CompressedBlob {
    stored: Vec<u8>,
    raw_len: usize,
    compression: u8,
}

fn compress_blob(raw: Vec<u8>) -> CompressedBlob {
    let raw_len = raw.len();
    let compressed = block::compress(&raw);
    if compressed.len() + 5 < raw_len {
        CompressedBlob {
            stored: compressed,
            raw_len,
            compression: ROW_GROUP_COMPRESSION_LZ4,
        }
    } else {
        CompressedBlob {
            stored: raw,
            raw_len,
            compression: ROW_GROUP_COMPRESSION_RAW,
        }
    }
}

fn decode_row_group_source(
    payload: &[u8],
    schema: &Schema,
    rows: usize,
    data_base_offset: usize,
) -> Result<(Vec<ColumnStats>, Vec<StoredColumnSource>)> {
    let mut cursor = 0;
    let mut stats = Vec::with_capacity(schema.columns.len());
    let mut columns = Vec::with_capacity(schema.columns.len());

    for column in &schema.columns {
        let column_stats = decode_column_stats(payload, &mut cursor, column.data_type)?;
        let nulls = decode_optional_blob_descriptor(payload, &mut cursor, data_base_offset)?;
        let data = decode_blob_descriptor(payload, &mut cursor, data_base_offset)?;
        stats.push(column_stats);
        columns.push(StoredColumnSource::new(nulls, data));
    }

    ensure!(
        cursor == payload.len(),
        "row group metadata had trailing bytes"
    );
    let _ = rows;
    Ok((stats, columns))
}

fn encode_column_stats(
    bytes: &mut Vec<u8>,
    stats: &ColumnStats,
    data_type: DataType,
) -> Result<()> {
    write_u32(bytes, stats.null_count as u32);
    encode_optional_stat_value(bytes, data_type, stats.min.as_ref())?;
    encode_optional_stat_value(bytes, data_type, stats.max.as_ref())?;
    Ok(())
}

fn decode_column_stats(
    payload: &[u8],
    cursor: &mut usize,
    data_type: DataType,
) -> Result<ColumnStats> {
    let null_count = read_u32(payload, cursor)? as usize;
    let min = decode_optional_stat_value(payload, cursor, data_type)?;
    let max = decode_optional_stat_value(payload, cursor, data_type)?;
    Ok(ColumnStats {
        min,
        max,
        null_count,
    })
}

fn encode_optional_stat_value(
    bytes: &mut Vec<u8>,
    data_type: DataType,
    value: Option<&Value>,
) -> Result<()> {
    match value {
        Some(value) => {
            bytes.push(1);
            encode_stat_value(bytes, data_type, value)
        }
        None => {
            bytes.push(0);
            Ok(())
        }
    }
}

fn decode_optional_stat_value(
    payload: &[u8],
    cursor: &mut usize,
    data_type: DataType,
) -> Result<Option<Value>> {
    match read_u8(payload, cursor)? {
        0 => Ok(None),
        1 => Ok(Some(decode_stat_value(payload, cursor, data_type)?)),
        other => bail!("unknown stat flag: {other}"),
    }
}

fn encode_stat_value(bytes: &mut Vec<u8>, data_type: DataType, value: &Value) -> Result<()> {
    match (data_type, value) {
        (DataType::Int64 | DataType::Timestamp, Value::Int64(value)) => {
            bytes.extend_from_slice(&value.to_le_bytes());
        }
        (DataType::Float64, Value::Float64(value)) => {
            bytes.extend_from_slice(&value.into_inner().to_le_bytes());
        }
        (DataType::Bool, Value::Bool(value)) => bytes.push(u8::from(*value)),
        (DataType::String, Value::String(value)) => write_string(bytes, value)?,
        (_, Value::Null) => bail!("NULL is not a valid stat value"),
        (_, other) => bail!("stat value type mismatch: {other:?}"),
    }
    Ok(())
}

fn decode_stat_value(payload: &[u8], cursor: &mut usize, data_type: DataType) -> Result<Value> {
    match data_type {
        DataType::Int64 | DataType::Timestamp => Ok(Value::Int64(read_i64(payload, cursor)?)),
        DataType::Float64 => Ok(Value::Float64(OrderedFloat(read_f64(payload, cursor)?))),
        DataType::Bool => Ok(Value::Bool(read_u8(payload, cursor)? != 0)),
        DataType::String => Ok(Value::String(read_string(payload, cursor)?)),
    }
}

fn encode_optional_blob_descriptor(bytes: &mut Vec<u8>, blob: Option<StoredBlob>) -> Result<()> {
    match blob {
        Some(blob) => {
            bytes.push(1);
            encode_blob_descriptor(bytes, blob);
        }
        None => bytes.push(0),
    }
    Ok(())
}

fn decode_optional_blob_descriptor(
    payload: &[u8],
    cursor: &mut usize,
    data_base_offset: usize,
) -> Result<Option<StoredBlob>> {
    match read_u8(payload, cursor)? {
        0 => Ok(None),
        1 => Ok(Some(decode_blob_descriptor(
            payload,
            cursor,
            data_base_offset,
        )?)),
        other => bail!("unknown optional blob flag: {other}"),
    }
}

fn encode_blob_descriptor(bytes: &mut Vec<u8>, blob: StoredBlob) {
    write_u32(bytes, blob.offset as u32);
    write_u32(bytes, blob.stored_len as u32);
    write_u32(bytes, blob.raw_len as u32);
    bytes.push(blob.compression);
}

fn decode_blob_descriptor(
    payload: &[u8],
    cursor: &mut usize,
    data_base_offset: usize,
) -> Result<StoredBlob> {
    Ok(StoredBlob {
        offset: data_base_offset + read_u32(payload, cursor)? as usize,
        stored_len: read_u32(payload, cursor)? as usize,
        raw_len: read_u32(payload, cursor)? as usize,
        compression: read_u8(payload, cursor)?,
    })
}

fn write_string(bytes: &mut Vec<u8>, value: &str) -> Result<()> {
    write_u32(bytes, value.len() as u32);
    bytes.extend_from_slice(value.as_bytes());
    Ok(())
}

fn read_string(payload: &[u8], cursor: &mut usize) -> Result<String> {
    let len = read_u32(payload, cursor)? as usize;
    ensure!(*cursor + len <= payload.len(), "string out of bounds");
    let value = std::str::from_utf8(&payload[*cursor..*cursor + len])?.to_string();
    *cursor += len;
    Ok(value)
}

fn read_u32_from_reader<R: Read>(reader: &mut R) -> Result<u32> {
    let mut buf = [0_u8; 4];
    reader.read_exact(&mut buf)?;
    Ok(u32::from_le_bytes(buf))
}

fn read_string_from_reader<R: Read>(reader: &mut R) -> Result<String> {
    let len = read_u32_from_reader(reader)? as usize;
    let mut buf = vec![0_u8; len];
    reader.read_exact(&mut buf)?;
    Ok(String::from_utf8(buf)?)
}

fn write_u16(bytes: &mut Vec<u8>, value: u16) {
    bytes.extend_from_slice(&value.to_le_bytes());
}

fn write_u32(bytes: &mut Vec<u8>, value: u32) {
    bytes.extend_from_slice(&value.to_le_bytes());
}

fn read_u8(payload: &[u8], cursor: &mut usize) -> Result<u8> {
    ensure!(*cursor < payload.len(), "read past end of payload");
    let value = payload[*cursor];
    *cursor += 1;
    Ok(value)
}

fn read_u16(payload: &[u8], cursor: &mut usize) -> Result<u16> {
    ensure!(*cursor + 2 <= payload.len(), "read past end of payload");
    let mut buf = [0_u8; 2];
    buf.copy_from_slice(&payload[*cursor..*cursor + 2]);
    *cursor += 2;
    Ok(u16::from_le_bytes(buf))
}

fn read_u32(payload: &[u8], cursor: &mut usize) -> Result<u32> {
    ensure!(*cursor + 4 <= payload.len(), "read past end of payload");
    let mut buf = [0_u8; 4];
    buf.copy_from_slice(&payload[*cursor..*cursor + 4]);
    *cursor += 4;
    Ok(u32::from_le_bytes(buf))
}

fn encode_int64_values(bytes: &mut Vec<u8>, values: &[i64]) {
    if values.is_empty() {
        bytes.push(INT_ENCODING_RAW);
        return;
    }

    let min = *values.iter().min().expect("non-empty values have a min");
    let max = *values.iter().max().expect("non-empty values have a max");
    let range = (max as i128 - min as i128) as u128;

    if range <= u8::MAX as u128 {
        bytes.push(INT_ENCODING_BASE_U8);
        bytes.extend_from_slice(&min.to_le_bytes());
        for value in values {
            bytes.push(((*value as i128 - min as i128) as u8).to_le());
        }
        return;
    }

    if range <= u16::MAX as u128 {
        bytes.push(INT_ENCODING_BASE_U16);
        bytes.extend_from_slice(&min.to_le_bytes());
        for value in values {
            bytes.extend_from_slice(&((*value as i128 - min as i128) as u16).to_le_bytes());
        }
        return;
    }

    if range <= u32::MAX as u128 {
        bytes.push(INT_ENCODING_BASE_U32);
        bytes.extend_from_slice(&min.to_le_bytes());
        for value in values {
            bytes.extend_from_slice(&((*value as i128 - min as i128) as u32).to_le_bytes());
        }
        return;
    }

    bytes.push(INT_ENCODING_RAW);
    for value in values {
        bytes.extend_from_slice(&value.to_le_bytes());
    }
}

fn decode_int64_values(payload: &[u8], cursor: &mut usize, rows: usize) -> Result<Vec<i64>> {
    let encoding = read_u8(payload, cursor)?;
    match encoding {
        INT_ENCODING_RAW => {
            let mut values = Vec::with_capacity(rows);
            for _ in 0..rows {
                values.push(read_i64(payload, cursor)?);
            }
            Ok(values)
        }
        INT_ENCODING_BASE_U8 => {
            let base = read_i64(payload, cursor)?;
            let mut values = Vec::with_capacity(rows);
            for _ in 0..rows {
                values.push(base + read_u8(payload, cursor)? as i64);
            }
            Ok(values)
        }
        INT_ENCODING_BASE_U16 => {
            let base = read_i64(payload, cursor)?;
            let mut values = Vec::with_capacity(rows);
            for _ in 0..rows {
                values.push(base + read_u16(payload, cursor)? as i64);
            }
            Ok(values)
        }
        INT_ENCODING_BASE_U32 => {
            let base = read_i64(payload, cursor)?;
            let mut values = Vec::with_capacity(rows);
            for _ in 0..rows {
                values.push(base + read_u32(payload, cursor)? as i64);
            }
            Ok(values)
        }
        other => bail!("unknown int encoding: {other}"),
    }
}

fn encode_packed_bools(bytes: &mut Vec<u8>, values: impl IntoIterator<Item = bool>) {
    let values = values.into_iter().collect::<Vec<_>>();
    bytes.extend_from_slice(&NullBitmap::from_bools(&values).encode());
}

fn decode_packed_bools(payload: &[u8], cursor: &mut usize, rows: usize) -> Result<Vec<bool>> {
    let byte_len = rows.div_ceil(8);
    ensure!(
        *cursor + byte_len <= payload.len(),
        "packed bools out of bounds"
    );
    let bitmap = NullBitmap::decode(payload[*cursor..*cursor + byte_len].to_vec(), rows);
    *cursor += byte_len;
    Ok((0..rows).map(|index| bitmap.is_present(index)).collect())
}

fn encode_code_values(bytes: &mut Vec<u8>, dictionary_len: usize, codes: &[u32]) -> Result<()> {
    let encoding = if dictionary_len <= (u8::MAX as usize) + 1 {
        CODE_ENCODING_U8
    } else if dictionary_len <= (u16::MAX as usize) + 1 {
        CODE_ENCODING_U16
    } else {
        CODE_ENCODING_U32
    };
    bytes.push(encoding);

    match encoding {
        CODE_ENCODING_U8 => {
            for code in codes {
                let code = u8::try_from(*code).context("dictionary code overflow for u8")?;
                bytes.push(code);
            }
        }
        CODE_ENCODING_U16 => {
            for code in codes {
                let code = u16::try_from(*code).context("dictionary code overflow for u16")?;
                bytes.extend_from_slice(&code.to_le_bytes());
            }
        }
        CODE_ENCODING_U32 => {
            for code in codes {
                bytes.extend_from_slice(&code.to_le_bytes());
            }
        }
        _ => unreachable!("encoding chosen above"),
    }

    Ok(())
}

fn decode_code_values(payload: &[u8], cursor: &mut usize, rows: usize) -> Result<Vec<u32>> {
    let encoding = read_u8(payload, cursor)?;
    let mut codes = Vec::with_capacity(rows);
    match encoding {
        CODE_ENCODING_U8 => {
            for _ in 0..rows {
                codes.push(read_u8(payload, cursor)? as u32);
            }
        }
        CODE_ENCODING_U16 => {
            for _ in 0..rows {
                codes.push(read_u16(payload, cursor)? as u32);
            }
        }
        CODE_ENCODING_U32 => {
            for _ in 0..rows {
                codes.push(read_u32(payload, cursor)?);
            }
        }
        other => bail!("unknown code encoding: {other}"),
    }
    Ok(codes)
}

fn read_i64(payload: &[u8], cursor: &mut usize) -> Result<i64> {
    ensure!(*cursor + 8 <= payload.len(), "read past end of payload");
    let mut buf = [0_u8; 8];
    buf.copy_from_slice(&payload[*cursor..*cursor + 8]);
    *cursor += 8;
    Ok(i64::from_le_bytes(buf))
}

fn read_f64(payload: &[u8], cursor: &mut usize) -> Result<f64> {
    ensure!(*cursor + 8 <= payload.len(), "read past end of payload");
    let mut buf = [0_u8; 8];
    buf.copy_from_slice(&payload[*cursor..*cursor + 8]);
    *cursor += 8;
    Ok(f64::from_le_bytes(buf))
}
