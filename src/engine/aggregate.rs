use std::cmp::Ordering;

use anyhow::{Context, Result, bail, ensure};
use ordered_float::OrderedFloat;

use crate::sql::AggregateKind;
use crate::storage::LoadedRowGroup;
use crate::types::Value;

use super::compile::{CompiledProjection, CompiledProjectionExpr};

#[derive(Debug, Clone)]
pub(super) enum AggState {
    Passthrough,
    Count(i64),
    Sum(f64),
    Min(Option<Value>),
    Max(Option<Value>),
    Avg { sum: f64, count: i64 },
}

impl AggState {
    pub(super) fn new(projection: &CompiledProjection) -> Self {
        match projection.expr {
            CompiledProjectionExpr::Column { .. } | CompiledProjectionExpr::Scalar(_) => {
                Self::Passthrough
            }
            CompiledProjectionExpr::Aggregate { kind, .. } => match kind {
                AggregateKind::Count => Self::Count(0),
                AggregateKind::Sum => Self::Sum(0.0),
                AggregateKind::Min => Self::Min(None),
                AggregateKind::Max => Self::Max(None),
                AggregateKind::Avg => Self::Avg { sum: 0.0, count: 0 },
            },
        }
    }

    pub(super) fn update(
        &mut self,
        projection: &CompiledProjection,
        row_group: &LoadedRowGroup<'_>,
        row: usize,
    ) -> Result<()> {
        match (self, &projection.expr) {
            (Self::Passthrough, CompiledProjectionExpr::Column { .. })
            | (Self::Passthrough, CompiledProjectionExpr::Scalar(_)) => Ok(()),
            (
                Self::Count(count),
                CompiledProjectionExpr::Aggregate {
                    kind: AggregateKind::Count,
                    ..
                },
            ) => {
                *count += 1;
                Ok(())
            }
            (
                Self::Sum(sum),
                CompiledProjectionExpr::Aggregate {
                    kind: AggregateKind::Sum,
                    column_index: Some(index),
                },
            ) => {
                *sum += row_group
                    .column(*index)
                    .numeric_at(row)
                    .context("SUM expects a numeric column")?;
                Ok(())
            }
            (
                Self::Min(current),
                CompiledProjectionExpr::Aggregate {
                    kind: AggregateKind::Min,
                    column_index: Some(index),
                },
            ) => {
                let value = row_group.column(*index).value_at(row);
                if current
                    .as_ref()
                    .is_none_or(|existing| value.compare(existing) == Some(Ordering::Less))
                {
                    *current = Some(value);
                }
                Ok(())
            }
            (
                Self::Max(current),
                CompiledProjectionExpr::Aggregate {
                    kind: AggregateKind::Max,
                    column_index: Some(index),
                },
            ) => {
                let value = row_group.column(*index).value_at(row);
                if current
                    .as_ref()
                    .is_none_or(|existing| value.compare(existing) == Some(Ordering::Greater))
                {
                    *current = Some(value);
                }
                Ok(())
            }
            (
                Self::Avg { sum, count },
                CompiledProjectionExpr::Aggregate {
                    kind: AggregateKind::Avg,
                    column_index: Some(index),
                },
            ) => {
                *sum += row_group
                    .column(*index)
                    .numeric_at(row)
                    .context("AVG expects a numeric column")?;
                *count += 1;
                Ok(())
            }
            _ => bail!("invalid aggregate configuration"),
        }
    }

    pub(super) fn merge(&mut self, other: AggState) -> Result<()> {
        match (self, other) {
            (Self::Passthrough, Self::Passthrough) => Ok(()),
            (Self::Count(lhs), Self::Count(rhs)) => {
                *lhs += rhs;
                Ok(())
            }
            (Self::Sum(lhs), Self::Sum(rhs)) => {
                *lhs += rhs;
                Ok(())
            }
            (Self::Min(lhs), Self::Min(rhs)) => {
                if let Some(rhs) = rhs {
                    if lhs
                        .as_ref()
                        .is_none_or(|existing| rhs.compare(existing) == Some(Ordering::Less))
                    {
                        *lhs = Some(rhs);
                    }
                }
                Ok(())
            }
            (Self::Max(lhs), Self::Max(rhs)) => {
                if let Some(rhs) = rhs {
                    if lhs
                        .as_ref()
                        .is_none_or(|existing| rhs.compare(existing) == Some(Ordering::Greater))
                    {
                        *lhs = Some(rhs);
                    }
                }
                Ok(())
            }
            (
                Self::Avg {
                    sum: lhs_sum,
                    count: lhs_count,
                },
                Self::Avg {
                    sum: rhs_sum,
                    count: rhs_count,
                },
            ) => {
                *lhs_sum += rhs_sum;
                *lhs_count += rhs_count;
                Ok(())
            }
            _ => bail!("aggregate merge type mismatch"),
        }
    }

    pub(super) fn finish(self) -> Result<Value> {
        match self {
            Self::Passthrough => bail!("passthrough state does not produce a value"),
            Self::Count(count) => Ok(Value::Int64(count)),
            Self::Sum(sum) => Ok(Value::Float64(OrderedFloat(sum))),
            Self::Min(Some(value)) | Self::Max(Some(value)) => Ok(value),
            Self::Min(None) | Self::Max(None) => bail!("aggregate had no values"),
            Self::Avg { sum, count } => {
                ensure!(count > 0, "AVG had no rows");
                Ok(Value::Float64(OrderedFloat(sum / count as f64)))
            }
        }
    }
}
