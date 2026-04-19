use anyhow::{Context, Result};
use ordered_float::OrderedFloat;

use crate::sql::{
    AggregateKind, ArithmeticOp, CompareOp, Filter, FilterExpr, Projection, ProjectionExpr,
    ScalarExpr,
};
use crate::storage::ColumnData;
use crate::types::{Schema, Value};

#[derive(Debug)]
pub(super) struct CompiledFilter {
    pub(super) column_index: usize,
    pub(super) op: CompareOp,
    pub(super) value: Value,
}

#[derive(Debug)]
pub(super) enum CompiledFilterExpr {
    Predicate(CompiledFilter),
    And(Vec<CompiledFilterExpr>),
    Or(Vec<CompiledFilterExpr>),
}

#[derive(Debug, Clone)]
pub(super) struct CompiledProjection {
    pub(super) alias: String,
    pub(super) expr: CompiledProjectionExpr,
}

#[derive(Debug, Clone)]
pub(super) enum CompiledScalarExpr {
    Literal(Value),
    ColumnRef(usize),
    BinaryOp {
        left: Box<CompiledScalarExpr>,
        op: ArithmeticOp,
        right: Box<CompiledScalarExpr>,
    },
    UnaryMinus(Box<CompiledScalarExpr>),
}

#[derive(Debug, Clone)]
pub(super) enum CompiledProjectionExpr {
    Column {
        column_index: usize,
        group_key_index: Option<usize>,
    },
    Scalar(CompiledScalarExpr),
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

pub(super) fn compile_filter_expr(
    schema: &Schema,
    filter: &FilterExpr,
) -> Result<CompiledFilterExpr> {
    match filter {
        FilterExpr::Predicate(filter) => Ok(CompiledFilterExpr::Predicate(compile_filter(
            schema, filter,
        )?)),
        FilterExpr::And(children) => {
            let mut compiled = children
                .iter()
                .map(|child| compile_filter_expr(schema, child))
                .collect::<Result<Vec<_>>>()?;
            compiled.sort_by_key(compiled_filter_priority);
            Ok(CompiledFilterExpr::And(compiled))
        }
        FilterExpr::Or(children) => Ok(CompiledFilterExpr::Or(
            children
                .iter()
                .map(|child| compile_filter_expr(schema, child))
                .collect::<Result<Vec<_>>>()?,
        )),
    }
}

fn compile_filter(schema: &Schema, filter: &Filter) -> Result<CompiledFilter> {
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
}

fn filter_priority(filter: &CompiledFilter) -> (u8, u8) {
    let op_priority = match filter.op {
        CompareOp::IsNull | CompareOp::IsNotNull => 0,
        CompareOp::Eq => 1,
        CompareOp::Gt | CompareOp::Gte | CompareOp::Lt | CompareOp::Lte => 2,
        CompareOp::NotEq => 3,
    };
    let value_priority = match filter.value {
        Value::Bool(_) => 0,
        Value::Int64(_) | Value::Float64(_) => 1,
        Value::String(_) => 2,
        Value::Null => 0,
    };
    (op_priority, value_priority)
}

fn compiled_filter_priority(filter: &CompiledFilterExpr) -> (u8, u8, u8) {
    match filter {
        CompiledFilterExpr::Predicate(filter) => {
            let (a, b) = filter_priority(filter);
            (0, a, b)
        }
        CompiledFilterExpr::And(_) => (1, 0, 0),
        CompiledFilterExpr::Or(_) => (2, 0, 0),
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
                ProjectionExpr::Scalar(scalar) => {
                    CompiledProjectionExpr::Scalar(compile_scalar_expr(schema, scalar)?)
                }
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

pub(super) fn required_column_indexes(
    schema_len: usize,
    filter: Option<&CompiledFilterExpr>,
    projections: &[CompiledProjection],
    extra_indexes: &[usize],
) -> Vec<usize> {
    let mut required = vec![false; schema_len];
    if let Some(filter) = filter {
        collect_filter_columns(filter, &mut required);
    }
    for projection in projections {
        match &projection.expr {
            CompiledProjectionExpr::Column { column_index, .. } => required[*column_index] = true,
            CompiledProjectionExpr::Scalar(scalar) => collect_scalar_columns(scalar, &mut required),
            CompiledProjectionExpr::Aggregate {
                column_index: Some(index),
                ..
            } => required[*index] = true,
            CompiledProjectionExpr::Aggregate {
                column_index: None, ..
            } => {}
        }
    }
    for &index in extra_indexes {
        required[index] = true;
    }
    required
        .into_iter()
        .enumerate()
        .filter_map(|(index, include)| include.then_some(index))
        .collect()
}

fn collect_filter_columns(filter: &CompiledFilterExpr, required: &mut [bool]) {
    match filter {
        CompiledFilterExpr::Predicate(filter) => required[filter.column_index] = true,
        CompiledFilterExpr::And(children) | CompiledFilterExpr::Or(children) => {
            for child in children {
                collect_filter_columns(child, required);
            }
        }
    }
}

fn compile_scalar_expr(schema: &Schema, expr: &ScalarExpr) -> Result<CompiledScalarExpr> {
    match expr {
        ScalarExpr::Literal(value) => Ok(CompiledScalarExpr::Literal(value.clone())),
        ScalarExpr::ColumnRef(name) => {
            Ok(CompiledScalarExpr::ColumnRef(column_index(schema, name)?))
        }
        ScalarExpr::BinaryOp { left, op, right } => Ok(CompiledScalarExpr::BinaryOp {
            left: Box::new(compile_scalar_expr(schema, left)?),
            op: *op,
            right: Box::new(compile_scalar_expr(schema, right)?),
        }),
        ScalarExpr::UnaryMinus(inner) => Ok(CompiledScalarExpr::UnaryMinus(Box::new(
            compile_scalar_expr(schema, inner)?,
        ))),
    }
}

fn collect_scalar_columns(expr: &CompiledScalarExpr, required: &mut [bool]) {
    match expr {
        CompiledScalarExpr::Literal(_) => {}
        CompiledScalarExpr::ColumnRef(index) => required[*index] = true,
        CompiledScalarExpr::BinaryOp { left, right, .. } => {
            collect_scalar_columns(left, required);
            collect_scalar_columns(right, required);
        }
        CompiledScalarExpr::UnaryMinus(inner) => collect_scalar_columns(inner, required),
    }
}
