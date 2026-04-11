use anyhow::{Context, Result};
use ordered_float::OrderedFloat;

use crate::sql::{AggregateKind, CompareOp, Filter, Projection, ProjectionExpr};
use crate::storage::ColumnData;
use crate::types::{Schema, Value};

#[derive(Debug)]
pub(super) struct CompiledFilter {
    pub(super) column_index: usize,
    pub(super) op: CompareOp,
    pub(super) value: Value,
}

#[derive(Debug)]
pub(super) enum RowSelection {
    All(usize),
    Indexes(Vec<u32>),
}

impl RowSelection {
    pub(super) fn is_empty(&self) -> bool {
        match self {
            Self::All(rows) => *rows == 0,
            Self::Indexes(rows) => rows.is_empty(),
        }
    }

    pub(super) fn try_for_each<F>(&self, mut visitor: F) -> Result<()>
    where
        F: FnMut(usize) -> Result<()>,
    {
        match self {
            Self::All(rows) => {
                for row in 0..*rows {
                    visitor(row)?;
                }
            }
            Self::Indexes(rows) => {
                for row in rows {
                    visitor(*row as usize)?;
                }
            }
        }
        Ok(())
    }
}

#[derive(Debug, Clone)]
pub(super) struct CompiledProjection {
    pub(super) alias: String,
    pub(super) expr: CompiledProjectionExpr,
}

#[derive(Debug, Clone)]
pub(super) enum CompiledProjectionExpr {
    Column {
        column_index: usize,
        group_key_index: Option<usize>,
    },
    Aggregate {
        kind: AggregateKind,
        column_index: Option<usize>,
    },
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub(super) enum LocalKeyPart {
    Int64(i64),
    Float64(OrderedFloat<f64>),
    Bool(bool),
    String(String),
    StringCode(u32),
}

impl LocalKeyPart {
    pub(super) fn from_column(column: &ColumnData, row: usize) -> Result<Self> {
        match column {
            ColumnData::Int64(values) => Ok(Self::Int64(values[row])),
            ColumnData::Float64(values) => Ok(Self::Float64(values[row])),
            ColumnData::Bool(values) => Ok(Self::Bool(values[row])),
            ColumnData::StringPlain(values) => Ok(Self::String(values[row].clone())),
            ColumnData::StringDict { codes, .. } => Ok(Self::StringCode(codes[row])),
        }
    }
}

pub(super) fn compile_filters(schema: &Schema, filters: &[Filter]) -> Result<Vec<CompiledFilter>> {
    let mut compiled = filters
        .iter()
        .map(|filter| {
            let column_index = column_index(schema, &filter.column)?;
            let data_type = schema.columns[column_index].data_type;
            let value = match filter.value.clone() {
                Some(v) => Value::cast_for(data_type, v)?,
                None => Value::Null,
            };
            Ok(CompiledFilter {
                column_index,
                op: filter.op,
                value,
            })
        })
        .collect::<Result<Vec<_>>>()?;
    compiled.sort_by_key(filter_priority);
    Ok(compiled)
}

fn filter_priority(filter: &CompiledFilter) -> u8 {
    match filter.op {
        CompareOp::Eq => 0,
        CompareOp::Gt | CompareOp::Gte | CompareOp::Lt | CompareOp::Lte => 1,
        CompareOp::NotEq => 2,
        CompareOp::IsNull | CompareOp::IsNotNull => 0,
    }
}

pub(super) fn compile_projections(
    schema: &Schema,
    group_by: &[String],
    projections: &[Projection],
) -> Result<Vec<CompiledProjection>> {
    projections
        .iter()
        .map(|projection| {
            let expr = match &projection.expr {
                ProjectionExpr::Column(name) => CompiledProjectionExpr::Column {
                    column_index: column_index(schema, name)?,
                    group_key_index: group_by
                        .iter()
                        .position(|group_column| group_column == name),
                },
                ProjectionExpr::Aggregate { kind, column } => CompiledProjectionExpr::Aggregate {
                    kind: *kind,
                    column_index: column
                        .as_ref()
                        .map(|name| column_index(schema, name))
                        .transpose()?,
                },
            };
            Ok(CompiledProjection {
                alias: projection.alias.clone(),
                expr,
            })
        })
        .collect()
}

pub(super) fn column_index(schema: &Schema, column_name: &str) -> Result<usize> {
    schema
        .columns
        .iter()
        .position(|column| column.name == column_name)
        .with_context(|| format!("unknown column {column_name}"))
}
