use std::cmp::Ordering;
use std::fmt;

use anyhow::{Result, bail};
use ordered_float::OrderedFloat;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DataType {
    Int64,
    Float64,
    Bool,
    String,
    Timestamp,
}

impl DataType {
    pub fn from_sql_name(name: &str) -> Result<Self> {
        let normalized = name.trim().to_ascii_uppercase();
        match normalized.as_str() {
            "INT" | "INTEGER" | "BIGINT" => Ok(Self::Int64),
            "REAL" | "FLOAT" | "DOUBLE" | "DOUBLE PRECISION" => Ok(Self::Float64),
            "BOOL" | "BOOLEAN" => Ok(Self::Bool),
            "TEXT" | "STRING" | "VARCHAR" | "CHAR" | "JSON" => Ok(Self::String),
            "TIMESTAMP" | "DATETIME" => Ok(Self::Timestamp),
            other => bail!("unsupported type: {other}"),
        }
    }

    pub fn tag(self) -> u8 {
        match self {
            Self::Int64 => 1,
            Self::Float64 => 2,
            Self::Bool => 3,
            Self::String => 4,
            Self::Timestamp => 5,
        }
    }

    pub fn from_tag(tag: u8) -> Result<Self> {
        match tag {
            1 => Ok(Self::Int64),
            2 => Ok(Self::Float64),
            3 => Ok(Self::Bool),
            4 => Ok(Self::String),
            5 => Ok(Self::Timestamp),
            other => bail!("unknown type tag: {other}"),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ColumnDef {
    pub name: String,
    pub data_type: DataType,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Schema {
    pub table_name: String,
    pub columns: Vec<ColumnDef>,
}

#[derive(Debug, Clone)]
pub enum BatchColumn {
    Int64(Vec<i64>),
    Float64(Vec<f64>),
    Bool(Vec<bool>),
    String(Vec<String>),
    Timestamp(Vec<i64>),
}

impl BatchColumn {
    pub fn len(&self) -> usize {
        match self {
            Self::Int64(values) => values.len(),
            Self::Float64(values) => values.len(),
            Self::Bool(values) => values.len(),
            Self::String(values) => values.len(),
            Self::Timestamp(values) => values.len(),
        }
    }

    pub fn data_type(&self) -> DataType {
        match self {
            Self::Int64(_) => DataType::Int64,
            Self::Float64(_) => DataType::Float64,
            Self::Bool(_) => DataType::Bool,
            Self::String(_) => DataType::String,
            Self::Timestamp(_) => DataType::Timestamp,
        }
    }

    pub(crate) fn take_prefix(&mut self, len: usize) -> Self {
        match self {
            Self::Int64(values) => {
                let tail = values.split_off(len);
                let head = std::mem::replace(values, tail);
                Self::Int64(head)
            }
            Self::Float64(values) => {
                let tail = values.split_off(len);
                let head = std::mem::replace(values, tail);
                Self::Float64(head)
            }
            Self::Bool(values) => {
                let tail = values.split_off(len);
                let head = std::mem::replace(values, tail);
                Self::Bool(head)
            }
            Self::String(values) => {
                let tail = values.split_off(len);
                let head = std::mem::replace(values, tail);
                Self::String(head)
            }
            Self::Timestamp(values) => {
                let tail = values.split_off(len);
                let head = std::mem::replace(values, tail);
                Self::Timestamp(head)
            }
        }
    }
}

#[derive(Debug, Clone)]
pub struct ColumnarBatch {
    columns: Vec<BatchColumn>,
    rows: usize,
}

impl ColumnarBatch {
    pub fn new(columns: Vec<BatchColumn>) -> Result<Self> {
        let rows = columns.first().map(BatchColumn::len).unwrap_or(0);
        for column in &columns {
            if column.len() != rows {
                bail!("column length mismatch in columnar batch")
            }
        }
        Ok(Self { columns, rows })
    }

    pub fn rows(&self) -> usize {
        self.rows
    }

    pub fn is_empty(&self) -> bool {
        self.rows == 0
    }

    pub fn columns(&self) -> &[BatchColumn] {
        &self.columns
    }

    pub fn validate_against(&self, schema: &Schema) -> Result<()> {
        if self.columns.len() != schema.columns.len() {
            bail!(
                "column count mismatch: expected {}, got {}",
                schema.columns.len(),
                self.columns.len()
            )
        }
        for (column, schema_column) in self.columns.iter().zip(&schema.columns) {
            if column.data_type() != schema_column.data_type {
                bail!(
                    "column type mismatch for {}: expected {:?}, got {:?}",
                    schema_column.name,
                    schema_column.data_type,
                    column.data_type()
                )
            }
        }
        Ok(())
    }

    pub(crate) fn take_prefix(&mut self, len: usize) -> Result<Self> {
        if len > self.rows {
            bail!("requested prefix {len} exceeds batch size {}", self.rows)
        }
        let columns = self
            .columns
            .iter_mut()
            .map(|column| column.take_prefix(len))
            .collect();
        self.rows -= len;
        Ok(Self { columns, rows: len })
    }

    pub(crate) fn into_columns(self) -> Vec<BatchColumn> {
        self.columns
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum Value {
    Int64(i64),
    Float64(OrderedFloat<f64>),
    Bool(bool),
    String(String),
    Null,
}

impl Value {
    pub fn as_i64(&self) -> Option<i64> {
        match self {
            Self::Int64(value) => Some(*value),
            _ => None,
        }
    }

    pub fn as_f64(&self) -> Option<f64> {
        match self {
            Self::Int64(value) => Some(*value as f64),
            Self::Float64(value) => Some(value.into_inner()),
            _ => None,
        }
    }

    pub fn as_str(&self) -> Option<&str> {
        match self {
            Self::String(s) => Some(s),
            _ => None,
        }
    }

    pub fn as_bool(&self) -> Option<bool> {
        match self {
            Self::Bool(b) => Some(*b),
            _ => None,
        }
    }

    pub fn is_null(&self) -> bool {
        matches!(self, Self::Null)
    }

    pub fn compare(&self, other: &Self) -> Option<Ordering> {
        match (self, other) {
            (Self::Null, _) | (_, Self::Null) => None,
            (Self::Int64(lhs), Self::Int64(rhs)) => Some(lhs.cmp(rhs)),
            (Self::Float64(lhs), Self::Float64(rhs)) => Some(lhs.cmp(rhs)),
            (Self::Int64(lhs), Self::Float64(rhs)) => Some(OrderedFloat(*lhs as f64).cmp(rhs)),
            (Self::Float64(lhs), Self::Int64(rhs)) => Some(lhs.cmp(&OrderedFloat(*rhs as f64))),
            (Self::Bool(lhs), Self::Bool(rhs)) => Some(lhs.cmp(rhs)),
            (Self::String(lhs), Self::String(rhs)) => Some(lhs.cmp(rhs)),
            _ => None,
        }
    }

    pub fn cast_for(data_type: DataType, value: Value) -> Result<Value> {
        match (data_type, value) {
            (_, Value::Null) => Ok(Value::Null),
            (DataType::Int64 | DataType::Timestamp, Value::Int64(value)) => Ok(Value::Int64(value)),
            (DataType::Float64, Value::Float64(value)) => Ok(Value::Float64(value)),
            (DataType::Float64, Value::Int64(value)) => {
                Ok(Value::Float64(OrderedFloat(value as f64)))
            }
            (DataType::Bool, Value::Bool(value)) => Ok(Value::Bool(value)),
            (DataType::String, Value::String(value)) => Ok(Value::String(value)),
            (expected, actual) => bail!("type mismatch: expected {expected:?}, got {actual:?}"),
        }
    }
}

impl fmt::Display for Value {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Int64(value) => write!(f, "{value}"),
            Self::Float64(value) => write!(f, "{}", value.into_inner()),
            Self::Bool(value) => write!(f, "{value}"),
            Self::String(value) => write!(f, "{value}"),
            Self::Null => write!(f, "NULL"),
        }
    }
}
