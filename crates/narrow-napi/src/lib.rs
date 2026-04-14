use std::time::Duration;

use napi::bindgen_prelude::*;
use napi_derive::napi;

use narrowdb::{
    BatchColumn, ColumnDef, ColumnarBatch, DataType, DbOptions, NarrowDb, Schema, Value,
};
use ordered_float::OrderedFloat;

fn to_napi_err(e: anyhow::Error) -> napi::Error {
    napi::Error::new(napi::Status::GenericFailure, format!("{e:#}"))
}

// ---------------------------------------------------------------------------
// JS-facing data types
// ---------------------------------------------------------------------------

#[napi(string_enum)]
pub enum JsDataType {
    Int64,
    Float64,
    Bool,
    String,
    Timestamp,
}

impl From<JsDataType> for DataType {
    fn from(dt: JsDataType) -> Self {
        match dt {
            JsDataType::Int64 => DataType::Int64,
            JsDataType::Float64 => DataType::Float64,
            JsDataType::Bool => DataType::Bool,
            JsDataType::String => DataType::String,
            JsDataType::Timestamp => DataType::Timestamp,
        }
    }
}

impl From<DataType> for JsDataType {
    fn from(dt: DataType) -> Self {
        match dt {
            DataType::Int64 => JsDataType::Int64,
            DataType::Float64 => JsDataType::Float64,
            DataType::Bool => JsDataType::Bool,
            DataType::String => JsDataType::String,
            DataType::Timestamp => JsDataType::Timestamp,
        }
    }
}

#[napi(object)]
pub struct JsColumnDef {
    pub name: String,
    pub data_type: JsDataType,
}

#[napi(object)]
pub struct JsSchema {
    pub table_name: String,
    pub columns: Vec<JsColumnDef>,
}

#[napi(object)]
pub struct JsDbOptions {
    pub row_group_size: Option<u32>,
    pub sync_on_flush: Option<bool>,
    pub auto_flush_interval_ms: Option<u32>,
}

#[napi(object)]
pub struct JsQueryResult {
    pub columns: Vec<String>,
    pub rows: Vec<Vec<JsValue>>,
}

#[napi(object)]
pub struct JsBatchColumn {
    pub column_type: JsDataType,
    pub int64_values: Option<Vec<i64>>,
    pub float64_values: Option<Vec<f64>>,
    pub bool_values: Option<Vec<bool>>,
    pub string_values: Option<Vec<String>>,
}

#[napi(object)]
pub struct JsColumnarBatch {
    pub columns: Vec<JsBatchColumn>,
}

// ---------------------------------------------------------------------------
// Value conversion
// ---------------------------------------------------------------------------

/// A JavaScript value that can be null, boolean, number, or string.
/// napi-rs maps this to `string | number | boolean | null` in TypeScript.
#[napi(object)]
pub struct JsValue {
    /// "null" | "bool" | "int64" | "float64" | "string"
    #[napi(js_name = "type")]
    pub kind: String,
    pub int64: Option<i64>,
    pub float64: Option<f64>,
    pub bool_val: Option<bool>,
    pub string_val: Option<String>,
}

impl JsValue {
    fn from_value(v: &Value) -> Self {
        match v {
            Value::Int64(n) => JsValue {
                kind: "int64".to_string(),
                int64: Some(*n),
                float64: None,
                bool_val: None,
                string_val: None,
            },
            Value::Float64(f) => JsValue {
                kind: "float64".to_string(),
                int64: None,
                float64: Some(f.into_inner()),
                bool_val: None,
                string_val: None,
            },
            Value::Bool(b) => JsValue {
                kind: "bool".to_string(),
                int64: None,
                float64: None,
                bool_val: Some(*b),
                string_val: None,
            },
            Value::String(s) => JsValue {
                kind: "string".to_string(),
                int64: None,
                float64: None,
                bool_val: None,
                string_val: Some(s.clone()),
            },
            Value::Null => JsValue {
                kind: "null".to_string(),
                int64: None,
                float64: None,
                bool_val: None,
                string_val: None,
            },
        }
    }

    fn to_value(&self) -> Result<Value> {
        match self.kind.as_str() {
            "null" => Ok(Value::Null),
            "bool" => Ok(Value::Bool(self.bool_val.unwrap_or(false))),
            "int64" => Ok(Value::Int64(self.int64.unwrap_or(0))),
            "float64" => Ok(Value::Float64(OrderedFloat(self.float64.unwrap_or(0.0)))),
            "string" => Ok(Value::String(
                self.string_val.clone().unwrap_or_default(),
            )),
            other => Err(napi::Error::new(
                napi::Status::InvalidArg,
                format!("unknown value type: {other}"),
            )),
        }
    }
}

// ---------------------------------------------------------------------------
// Conversion helpers
// ---------------------------------------------------------------------------

fn convert_schema(schema: JsSchema) -> Schema {
    Schema {
        table_name: schema.table_name,
        columns: schema
            .columns
            .into_iter()
            .map(|c| ColumnDef {
                name: c.name,
                data_type: c.data_type.into(),
            })
            .collect(),
    }
}

fn convert_options(opts: Option<JsDbOptions>) -> DbOptions {
    match opts {
        None => DbOptions::default(),
        Some(o) => DbOptions {
            row_group_size: o.row_group_size.map(|v| v as usize).unwrap_or(16_384),
            sync_on_flush: o.sync_on_flush.unwrap_or(true),
            auto_flush_interval: o
                .auto_flush_interval_ms
                .map(|ms| Duration::from_millis(ms as u64)),
        },
    }
}

fn convert_row(row: Vec<JsValue>) -> Result<Vec<Value>> {
    row.into_iter().map(|v| v.to_value()).collect()
}

fn convert_query_result(qr: narrowdb::QueryResult) -> JsQueryResult {
    JsQueryResult {
        columns: qr.columns,
        rows: qr
            .rows
            .iter()
            .map(|row| row.iter().map(JsValue::from_value).collect())
            .collect(),
    }
}

fn convert_batch_column(bc: JsBatchColumn) -> Result<BatchColumn> {
    match bc.column_type {
        JsDataType::Int64 => Ok(BatchColumn::Int64(bc.int64_values.unwrap_or_default())),
        JsDataType::Float64 => Ok(BatchColumn::Float64(bc.float64_values.unwrap_or_default())),
        JsDataType::Bool => Ok(BatchColumn::Bool(bc.bool_values.unwrap_or_default())),
        JsDataType::String => Ok(BatchColumn::String(bc.string_values.unwrap_or_default())),
        JsDataType::Timestamp => Ok(BatchColumn::Timestamp(
            bc.int64_values.unwrap_or_default(),
        )),
    }
}

// ---------------------------------------------------------------------------
// NarrowDatabase class
// ---------------------------------------------------------------------------

#[napi]
pub struct NarrowDatabase {
    inner: NarrowDb,
}

#[napi]
impl NarrowDatabase {
    #[napi(factory)]
    pub fn open(path: String, options: Option<JsDbOptions>) -> Result<NarrowDatabase> {
        let db = NarrowDb::open(&path, convert_options(options)).map_err(to_napi_err)?;
        Ok(NarrowDatabase { inner: db })
    }

    #[napi(getter)]
    pub fn path(&self) -> Result<String> {
        self.inner
            .path()
            .map(|p| p.to_string_lossy().into_owned())
            .map_err(to_napi_err)
    }

    #[napi]
    pub fn create_table(&self, schema: JsSchema) -> Result<()> {
        self.inner
            .create_table(convert_schema(schema))
            .map_err(to_napi_err)
    }

    #[napi]
    pub fn execute(&self, sql: String) -> Result<Vec<JsQueryResult>> {
        self.inner
            .execute_sql(&sql)
            .map(|results| results.into_iter().map(convert_query_result).collect())
            .map_err(to_napi_err)
    }

    #[napi]
    pub fn execute_one(&self, sql: String) -> Result<JsQueryResult> {
        self.inner
            .execute_one(&sql)
            .map(convert_query_result)
            .map_err(to_napi_err)
    }

    #[napi]
    pub fn query(&self, sql: String) -> Result<Vec<JsQueryResult>> {
        self.inner
            .query(&sql)
            .map(|results| results.into_iter().map(convert_query_result).collect())
            .map_err(to_napi_err)
    }

    #[napi]
    pub fn insert_row(&self, table_name: String, row: Vec<JsValue>) -> Result<()> {
        let row = convert_row(row)?;
        self.inner.insert_row(&table_name, row).map_err(to_napi_err)
    }

    #[napi]
    pub fn insert_rows(&self, table_name: String, rows: Vec<Vec<JsValue>>) -> Result<()> {
        let rows: Vec<Vec<Value>> = rows
            .into_iter()
            .map(convert_row)
            .collect::<Result<Vec<_>>>()?;
        self.inner
            .insert_rows(&table_name, rows)
            .map_err(to_napi_err)
    }

    #[napi]
    pub fn insert_columnar_batch(
        &self,
        table_name: String,
        batch: JsColumnarBatch,
    ) -> Result<()> {
        let columns: Vec<BatchColumn> = batch
            .columns
            .into_iter()
            .map(convert_batch_column)
            .collect::<Result<Vec<_>>>()?;
        let batch = ColumnarBatch::new(columns).map_err(to_napi_err)?;
        self.inner
            .insert_columnar_batch(&table_name, batch)
            .map_err(to_napi_err)
    }

    #[napi]
    pub fn flush_table(&self, table_name: String) -> Result<()> {
        self.inner.flush_table(&table_name).map_err(to_napi_err)
    }

    #[napi]
    pub fn flush_all(&self) -> Result<()> {
        self.inner.flush_all().map_err(to_napi_err)
    }
}
