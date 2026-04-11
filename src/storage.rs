use std::cmp::Ordering;
use std::fs::{File, OpenOptions};
use std::io::{BufReader, BufWriter, Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};

use anyhow::{Context, Result, bail, ensure};
use memmap2::{Mmap, MmapOptions};
use once_cell::sync::OnceCell;
use ordered_float::OrderedFloat;
use rustc_hash::FxHashMap;

use crate::types::{BatchColumn, ColumnDef, ColumnarBatch, DataType, Schema, Value};

const MAGIC: &[u8; 8] = b"NRWDB003";
const RECORD_CREATE_TABLE: u8 = 1;
const RECORD_ROW_GROUP: u8 = 2;

#[derive(Debug, Clone)]
pub struct DbOptions {
    pub row_group_size: usize,
    pub sync_on_flush: bool,
}

impl Default for DbOptions {
    fn default() -> Self {
        Self {
            row_group_size: 16_384,
            sync_on_flush: true,
        }
    }
}

#[derive(Debug, Clone)]
pub struct ColumnStats {
    pub min: Option<Value>,
    pub max: Option<Value>,
}

impl ColumnStats {
    fn from_column(column: &ColumnData) -> Self {
        match column {
            ColumnData::Int64(values) => Self {
                min: values.iter().min().copied().map(Value::Int64),
                max: values.iter().max().copied().map(Value::Int64),
            },
            ColumnData::Float64(values) => Self {
                min: values.iter().min().copied().map(Value::Float64),
                max: values.iter().max().copied().map(Value::Float64),
            },
            ColumnData::Bool(values) => Self {
                min: values.iter().min().copied().map(Value::Bool),
                max: values.iter().max().copied().map(Value::Bool),
            },
            ColumnData::StringPlain(values) => Self {
                min: values.iter().min().cloned().map(Value::String),
                max: values.iter().max().cloned().map(Value::String),
            },
            ColumnData::StringDict { dictionary, codes } => {
                let mut min = None;
                let mut max = None;
                for code in codes {
                    let value = dictionary[*code as usize].clone();
                    if min.as_ref().is_none_or(|current: &String| value < *current) {
                        min = Some(value.clone());
                    }
                    if max.as_ref().is_none_or(|current: &String| value > *current) {
                        max = Some(value);
                    }
                }
                Self {
                    min: min.map(Value::String),
                    max: max.map(Value::String),
                }
            }
        }
    }
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
    pub stats: Vec<ColumnStats>,
}

impl RowGroup {
    fn new(columns: Vec<ColumnData>) -> Self {
        let rows = columns.first().map(ColumnData::len).unwrap_or(0);
        let stats = columns.iter().map(ColumnStats::from_column).collect();
        Self {
            rows,
            columns,
            stats,
        }
    }
}

#[derive(Debug)]
pub struct StoredRowGroup {
    decoded: OnceCell<RowGroup>,
    source: StoredRowGroupSource,
}

#[derive(Debug)]
enum StoredRowGroupSource {
    InMemory,
    Mapped { offset: usize, len: usize },
}

impl StoredRowGroup {
    pub fn from_row_group(row_group: RowGroup) -> Self {
        let decoded = OnceCell::new();
        decoded
            .set(row_group)
            .expect("fresh OnceLock accepts row group");
        Self {
            decoded,
            source: StoredRowGroupSource::InMemory,
        }
    }

    pub fn from_mapped(offset: usize, len: usize) -> Self {
        Self {
            decoded: OnceCell::new(),
            source: StoredRowGroupSource::Mapped { offset, len },
        }
    }

    pub fn get<'a>(&'a self, schema: &Schema, mapped: Option<&'a [u8]>) -> Result<&'a RowGroup> {
        if let Some(row_group) = self.decoded.get() {
            return Ok(row_group);
        }

        match self.source {
            StoredRowGroupSource::InMemory => self
                .decoded
                .get()
                .context("in-memory row group missing decoded state"),
            StoredRowGroupSource::Mapped { offset, len } => {
                let mapped = mapped.context("mapped storage unavailable for lazy row group")?;
                let payload = mapped
                    .get(offset..offset + len)
                    .context("row group payload out of bounds")?;
                self.decoded
                    .get_or_try_init(|| decode_row_group(payload, schema))
            }
        }
    }
}

#[derive(Debug, Clone)]
pub enum ColumnBuilder {
    Int64(Vec<i64>),
    Float64(Vec<OrderedFloat<f64>>),
    Bool(Vec<bool>),
    String(Vec<String>),
}

impl ColumnBuilder {
    pub fn with_capacity(data_type: DataType, capacity: usize) -> Self {
        match data_type {
            DataType::Int64 | DataType::Timestamp => Self::Int64(Vec::with_capacity(capacity)),
            DataType::Float64 => Self::Float64(Vec::with_capacity(capacity)),
            DataType::Bool => Self::Bool(Vec::with_capacity(capacity)),
            DataType::String => Self::String(Vec::with_capacity(capacity)),
        }
    }

    pub fn append(&mut self, value: Value) -> Result<()> {
        match (self, value) {
            (Self::Int64(values), Value::Int64(value)) => values.push(value),
            (Self::Float64(values), Value::Float64(value)) => values.push(value),
            (Self::Float64(values), Value::Int64(value)) => values.push(OrderedFloat(value as f64)),
            (Self::Bool(values), Value::Bool(value)) => values.push(value),
            (Self::String(values), Value::String(value)) => values.push(value),
            (builder, value) => bail!("cannot append {value:?} into {builder:?}"),
        }
        Ok(())
    }

    pub fn len(&self) -> usize {
        match self {
            Self::Int64(values) => values.len(),
            Self::Float64(values) => values.len(),
            Self::Bool(values) => values.len(),
            Self::String(values) => values.len(),
        }
    }

    pub fn finish(&mut self) -> ColumnData {
        match self {
            Self::Int64(values) => ColumnData::Int64(std::mem::take(values)),
            Self::Float64(values) => ColumnData::Float64(std::mem::take(values)),
            Self::Bool(values) => ColumnData::Bool(std::mem::take(values)),
            Self::String(values) => build_string_column(std::mem::take(values)),
        }
    }

    pub fn append_batch_column(&mut self, column: BatchColumn) -> Result<()> {
        match (self, column) {
            (Self::Int64(values), BatchColumn::Int64(mut batch))
            | (Self::Int64(values), BatchColumn::Timestamp(mut batch)) => values.append(&mut batch),
            (Self::Float64(values), BatchColumn::Float64(batch)) => {
                values.extend(batch.into_iter().map(OrderedFloat));
            }
            (Self::Bool(values), BatchColumn::Bool(mut batch)) => values.append(&mut batch),
            (Self::String(values), BatchColumn::String(mut batch)) => values.append(&mut batch),
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
        let columns = self.columns.iter_mut().map(ColumnBuilder::finish).collect();
        RowGroup::new(columns)
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
    Ok(RowGroup::new(columns))
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
                    let table = tables
                        .get_mut(&table_name)
                        .with_context(|| format!("missing table for row group {table_name}"))?;
                    table
                        .row_groups
                        .push(StoredRowGroup::from_mapped(payload_offset, len));
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
    for (column, column_def) in row_group.columns.iter().zip(&schema.columns) {
        match (column, column_def.data_type) {
            (ColumnData::Int64(values), DataType::Int64 | DataType::Timestamp) => {
                for value in values {
                    bytes.extend_from_slice(&value.to_le_bytes());
                }
            }
            (ColumnData::Float64(values), DataType::Float64) => {
                for value in values {
                    bytes.extend_from_slice(&value.into_inner().to_le_bytes());
                }
            }
            (ColumnData::Bool(values), DataType::Bool) => {
                for value in values {
                    bytes.push(u8::from(*value));
                }
            }
            (ColumnData::StringPlain(values), DataType::String) => {
                bytes.push(0);
                for value in values {
                    write_string(&mut bytes, value)?;
                }
            }
            (ColumnData::StringDict { dictionary, codes }, DataType::String) => {
                bytes.push(1);
                write_u32(&mut bytes, dictionary.len() as u32);
                for value in dictionary {
                    write_string(&mut bytes, value)?;
                }
                for code in codes {
                    write_u32(&mut bytes, *code);
                }
            }
            _ => bail!("column type mismatch while encoding row group"),
        }
    }
    Ok(bytes)
}

fn decode_row_group(payload: &[u8], schema: &Schema) -> Result<RowGroup> {
    let mut cursor = 0;
    let table_name = read_string(payload, &mut cursor)?;
    ensure!(table_name == schema.table_name, "row group table mismatch");
    let rows = read_u32(payload, &mut cursor)? as usize;
    let mut columns = Vec::with_capacity(schema.columns.len());

    for column in &schema.columns {
        let data = match column.data_type {
            DataType::Int64 | DataType::Timestamp => {
                let mut values = Vec::with_capacity(rows);
                for _ in 0..rows {
                    values.push(read_i64(payload, &mut cursor)?);
                }
                ColumnData::Int64(values)
            }
            DataType::Float64 => {
                let mut values = Vec::with_capacity(rows);
                for _ in 0..rows {
                    values.push(OrderedFloat(read_f64(payload, &mut cursor)?));
                }
                ColumnData::Float64(values)
            }
            DataType::Bool => {
                let mut values = Vec::with_capacity(rows);
                for _ in 0..rows {
                    values.push(read_u8(payload, &mut cursor)? != 0);
                }
                ColumnData::Bool(values)
            }
            DataType::String => match read_u8(payload, &mut cursor)? {
                0 => {
                    let mut values = Vec::with_capacity(rows);
                    for _ in 0..rows {
                        values.push(read_string(payload, &mut cursor)?);
                    }
                    ColumnData::StringPlain(values)
                }
                1 => {
                    let dictionary_len = read_u32(payload, &mut cursor)? as usize;
                    let mut dictionary = Vec::with_capacity(dictionary_len);
                    for _ in 0..dictionary_len {
                        dictionary.push(read_string(payload, &mut cursor)?);
                    }
                    let mut codes = Vec::with_capacity(rows);
                    for _ in 0..rows {
                        codes.push(read_u32(payload, &mut cursor)?);
                    }
                    ColumnData::StringDict { dictionary, codes }
                }
                other => bail!("unknown string column encoding: {other}"),
            },
        };
        columns.push(data);
    }

    Ok(RowGroup::new(columns))
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
