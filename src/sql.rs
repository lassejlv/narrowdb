use anyhow::{Result, anyhow, bail, ensure};
use ordered_float::OrderedFloat;
use sqlparser::ast::{
    BinaryOperator, Expr, FunctionArg, FunctionArgExpr, FunctionArguments, Ident, OrderByExpr,
    Query, Select, SelectItem, SetExpr, Statement, TableFactor, Value as SqlValue,
};
use sqlparser::dialect::SQLiteDialect;
use sqlparser::parser::Parser;

use crate::types::{DataType, Schema, Value};

#[derive(Debug)]
pub enum Command {
    CreateTable(Schema),
    Insert(InsertPlan),
    Select(SelectPlan),
    Eval(EvalPlan),
}

#[derive(Debug)]
pub struct InsertPlan {
    pub table_name: String,
    pub rows: Vec<Vec<Value>>,
}

#[derive(Debug, Clone, Copy)]
pub enum CompareOp {
    Eq,
    NotEq,
    Lt,
    Lte,
    Gt,
    Gte,
    IsNull,
    IsNotNull,
}

#[derive(Debug, Clone)]
pub struct Filter {
    pub column: String,
    pub op: CompareOp,
    pub value: Option<Value>,
}

#[derive(Debug, Clone, Copy)]
pub enum AggregateKind {
    Count,
    Sum,
    Min,
    Max,
    Avg,
}

#[derive(Debug, Clone)]
pub enum ProjectionExpr {
    Column(String),
    Scalar(ScalarExpr),
    Aggregate {
        kind: AggregateKind,
        column: Option<String>,
    },
}

#[derive(Debug, Clone)]
pub struct Projection {
    pub expr: ProjectionExpr,
    pub alias: String,
}

#[derive(Debug, Clone)]
pub struct OrderByPlan {
    pub field: String,
    pub descending: bool,
}

#[derive(Debug, Clone)]
pub struct SelectPlan {
    pub table_name: String,
    pub filters: Vec<Filter>,
    pub projections: Vec<Projection>,
    pub group_by: Vec<String>,
    pub order_by: Option<OrderByPlan>,
    pub limit: Option<usize>,
}

#[derive(Debug, Clone, Copy)]
pub enum ArithmeticOp {
    Add,
    Sub,
    Mul,
    Div,
    Mod,
}

#[derive(Debug, Clone)]
pub enum ScalarExpr {
    Literal(Value),
    ColumnRef(String),
    BinaryOp {
        left: Box<ScalarExpr>,
        op: ArithmeticOp,
        right: Box<ScalarExpr>,
    },
    UnaryMinus(Box<ScalarExpr>),
}

#[derive(Debug)]
pub struct EvalPlan {
    pub exprs: Vec<(ScalarExpr, String)>,
}

pub fn parse_sql(sql: &str) -> Result<Vec<Command>> {
    let dialect = SQLiteDialect {};
    let statements = Parser::parse_sql(&dialect, sql)?;
    statements.into_iter().map(parse_statement).collect()
}

fn parse_statement(statement: Statement) -> Result<Command> {
    match statement {
        Statement::CreateTable(create) => parse_create_table(create),
        Statement::Insert(insert) => parse_insert(insert),
        Statement::Query(query) => parse_query(*query),
        other => bail!("unsupported statement: {other}"),
    }
}

fn parse_create_table(statement: sqlparser::ast::CreateTable) -> Result<Command> {
    let table_name = object_name_to_string(&statement.name.0)?;
    let mut columns = Vec::with_capacity(statement.columns.len());
    for column in statement.columns {
        columns.push(crate::types::ColumnDef {
            name: column.name.value,
            data_type: DataType::from_sql_name(&column.data_type.to_string())?,
        });
    }
    Ok(Command::CreateTable(Schema {
        table_name,
        columns,
    }))
}

fn parse_insert(statement: sqlparser::ast::Insert) -> Result<Command> {
    ensure!(
        statement.columns.is_empty(),
        "explicit insert column lists are not supported yet"
    );
    let table_name = object_name_to_string(&statement.table_name.0)?;
    let Some(source) = statement.source else {
        bail!("INSERT source is required")
    };

    let rows = match *source.body {
        SetExpr::Values(values) => values
            .rows
            .into_iter()
            .map(|row| row.into_iter().map(parse_sql_value_expr).collect())
            .collect::<Result<Vec<Vec<Value>>>>()?,
        other => bail!("unsupported INSERT source: {other}"),
    };

    Ok(Command::Insert(InsertPlan { table_name, rows }))
}

fn parse_query(query: Query) -> Result<Command> {
    let select = match *query.body {
        SetExpr::Select(select) => select,
        other => bail!("unsupported query body: {other}"),
    };

    if select.from.is_empty() {
        return parse_eval(*select);
    }

    let plan = parse_select(*select, query.order_by, query.limit)?;
    Ok(Command::Select(plan))
}

fn parse_select(
    select: Select,
    order_by: Option<sqlparser::ast::OrderBy>,
    limit: Option<Expr>,
) -> Result<SelectPlan> {
    ensure!(select.from.len() == 1, "exactly one table is supported");
    let table_name = match &select.from[0].relation {
        TableFactor::Table { name, .. } => object_name_to_string(&name.0)?,
        other => bail!("unsupported table source: {other}"),
    };

    let filters = match select.selection {
        Some(expr) => parse_filters(expr)?,
        None => Vec::new(),
    };

    let group_by = match select.group_by {
        sqlparser::ast::GroupByExpr::Expressions(exprs, _) => exprs
            .into_iter()
            .map(parse_identifier_expr)
            .collect::<Result<Vec<String>>>()?,
        sqlparser::ast::GroupByExpr::All(_) => bail!("GROUP BY ALL is not supported"),
    };

    let projections = parse_projections(select.projection, &group_by)?;
    let order_by = parse_order_by(order_by)?;
    let limit = match limit {
        Some(expr) => Some(parse_usize_literal(expr)?),
        None => None,
    };

    Ok(SelectPlan {
        table_name,
        filters,
        projections,
        group_by,
        order_by,
        limit,
    })
}

fn parse_filters(expr: Expr) -> Result<Vec<Filter>> {
    match expr {
        Expr::BinaryOp {
            left,
            op: BinaryOperator::And,
            right,
        } => {
            let mut filters = parse_filters(*left)?;
            filters.extend(parse_filters(*right)?);
            Ok(filters)
        }
        Expr::BinaryOp { left, op, right } => {
            let column = parse_identifier_expr(*left)?;
            let value = parse_sql_value_expr(*right)?;
            let op = match op {
                BinaryOperator::Eq => CompareOp::Eq,
                BinaryOperator::NotEq => CompareOp::NotEq,
                BinaryOperator::Lt => CompareOp::Lt,
                BinaryOperator::LtEq => CompareOp::Lte,
                BinaryOperator::Gt => CompareOp::Gt,
                BinaryOperator::GtEq => CompareOp::Gte,
                other => bail!("unsupported filter operator: {other}"),
            };
            Ok(vec![Filter {
                column,
                op,
                value: Some(value),
            }])
        }
        Expr::IsNull(expr) => {
            let column = parse_identifier_expr(*expr)?;
            Ok(vec![Filter {
                column,
                op: CompareOp::IsNull,
                value: None,
            }])
        }
        Expr::IsNotNull(expr) => {
            let column = parse_identifier_expr(*expr)?;
            Ok(vec![Filter {
                column,
                op: CompareOp::IsNotNull,
                value: None,
            }])
        }
        other => bail!("unsupported WHERE expression: {other}"),
    }
}

fn parse_projections(items: Vec<SelectItem>, group_by: &[String]) -> Result<Vec<Projection>> {
    let mut projections = Vec::new();
    for item in items {
        match item {
            SelectItem::Wildcard(_) => projections.push(Projection {
                expr: ProjectionExpr::Column("*".to_string()),
                alias: "*".to_string(),
            }),
            SelectItem::UnnamedExpr(expr) => {
                projections.push(parse_projection_expr(expr, group_by, None)?)
            }
            SelectItem::ExprWithAlias { expr, alias } => {
                projections.push(parse_projection_expr(expr, group_by, Some(alias.value))?)
            }
            other => bail!("unsupported SELECT projection: {other}"),
        }
    }
    Ok(projections)
}

fn parse_projection_expr(
    expr: Expr,
    group_by: &[String],
    alias_override: Option<String>,
) -> Result<Projection> {
    match expr {
        Expr::Identifier(ident) => {
            let alias = alias_override.unwrap_or_else(|| ident.value.clone());
            if !group_by.is_empty() && !group_by.iter().any(|column| column == &ident.value) {
                bail!(
                    "non-aggregated projection {} must appear in GROUP BY",
                    ident.value
                );
            }
            Ok(Projection {
                expr: ProjectionExpr::Column(ident.value),
                alias,
            })
        }
        Expr::CompoundIdentifier(parts) => {
            let name = compound_identifier_to_string(&parts)?;
            let alias = alias_override.unwrap_or_else(|| name.clone());
            if !group_by.is_empty() && !group_by.iter().any(|column| column == &name) {
                bail!("non-aggregated projection {name} must appear in GROUP BY");
            }
            Ok(Projection {
                expr: ProjectionExpr::Column(name),
                alias,
            })
        }
        Expr::Function(function) => {
            let name = function.name.to_string().to_ascii_uppercase();
            let kind = match name.as_str() {
                "COUNT" => AggregateKind::Count,
                "SUM" => AggregateKind::Sum,
                "MIN" => AggregateKind::Min,
                "MAX" => AggregateKind::Max,
                "AVG" => AggregateKind::Avg,
                _ => bail!("unsupported aggregate: {name}"),
            };
            let column = parse_function_column(function.args)?;
            let alias = alias_override.unwrap_or_else(|| {
                if let Some(column) = &column {
                    format!("{}_{}", name.to_ascii_lowercase(), column)
                } else {
                    name.to_ascii_lowercase()
                }
            });
            Ok(Projection {
                expr: ProjectionExpr::Aggregate { kind, column },
                alias,
            })
        }
        other => {
            let alias = alias_override.unwrap_or_else(|| other.to_string());
            let scalar = parse_scalar_expr(other)?;
            Ok(Projection {
                expr: ProjectionExpr::Scalar(scalar),
                alias,
            })
        }
    }
}

fn parse_function_column(arguments: FunctionArguments) -> Result<Option<String>> {
    match arguments {
        FunctionArguments::None => Ok(None),
        FunctionArguments::Subquery(_) => bail!("subquery function arguments are not supported"),
        FunctionArguments::List(list) => {
            if list.args.len() == 1 {
                match &list.args[0] {
                    FunctionArg::Unnamed(FunctionArgExpr::Wildcard) => Ok(None),
                    FunctionArg::Unnamed(FunctionArgExpr::Expr(expr)) => {
                        Ok(Some(parse_identifier_expr(expr.clone())?))
                    }
                    other => bail!("unsupported function argument: {other}"),
                }
            } else {
                bail!("only single-argument aggregates are supported")
            }
        }
    }
}

fn parse_order_by(order_by: Option<sqlparser::ast::OrderBy>) -> Result<Option<OrderByPlan>> {
    let Some(order_by) = order_by else {
        return Ok(None);
    };
    ensure!(
        order_by.exprs.len() == 1,
        "only one ORDER BY expression is supported"
    );
    let OrderByExpr { expr, asc, .. } = order_by.exprs.into_iter().next().unwrap();
    Ok(Some(OrderByPlan {
        field: parse_identifier_expr(expr)?,
        descending: matches!(asc, Some(false)),
    }))
}

fn parse_identifier_expr(expr: Expr) -> Result<String> {
    match expr {
        Expr::Identifier(ident) => Ok(ident.value),
        Expr::CompoundIdentifier(parts) => compound_identifier_to_string(&parts),
        other => bail!("expected column identifier, got {other}"),
    }
}

fn compound_identifier_to_string(parts: &[Ident]) -> Result<String> {
    let Some(last) = parts.last() else {
        bail!("empty identifier")
    };
    Ok(last.value.clone())
}

fn parse_usize_literal(expr: Expr) -> Result<usize> {
    match parse_sql_value_expr(expr)? {
        Value::Int64(value) => Ok(value as usize),
        other => bail!("expected integer literal, got {other:?}"),
    }
}

fn parse_sql_value_expr(expr: Expr) -> Result<Value> {
    match expr {
        Expr::Value(value) => parse_sql_value(value),
        Expr::UnaryOp { op, expr } if op.to_string() == "-" => match parse_sql_value_expr(*expr)? {
            Value::Int64(value) => Ok(Value::Int64(-value)),
            Value::Float64(value) => Ok(Value::Float64(OrderedFloat(-value.into_inner()))),
            other => bail!("unsupported unary negation for {other:?}"),
        },
        Expr::IsNull(_) => {
            // IS NULL / IS NOT NULL handled as a special filter elsewhere
            bail!("IS NULL expressions must appear in WHERE clauses")
        }
        other => bail!("expected literal value, got {other}"),
    }
}

fn parse_sql_value(value: sqlparser::ast::Value) -> Result<Value> {
    match value {
        SqlValue::Number(number, _) => {
            if number.contains('.') {
                Ok(Value::Float64(OrderedFloat(number.parse::<f64>()?)))
            } else {
                Ok(Value::Int64(number.parse::<i64>()?))
            }
        }
        SqlValue::SingleQuotedString(value) | SqlValue::DoubleQuotedString(value) => {
            Ok(Value::String(value))
        }
        SqlValue::Boolean(value) => Ok(Value::Bool(value)),
        SqlValue::Null => Ok(Value::Null),
        other => Err(anyhow!("unsupported literal value: {other}")),
    }
}

fn object_name_to_string(parts: &[Ident]) -> Result<String> {
    let Some(last) = parts.last() else {
        bail!("empty object name")
    };
    Ok(last.value.clone())
}

fn parse_eval(select: Select) -> Result<Command> {
    let mut exprs = Vec::new();
    for item in select.projection {
        match item {
            SelectItem::UnnamedExpr(expr) => {
                let alias = expr.to_string();
                exprs.push((parse_scalar_expr(expr)?, alias));
            }
            SelectItem::ExprWithAlias { expr, alias } => {
                exprs.push((parse_scalar_expr(expr)?, alias.value));
            }
            other => bail!("unsupported expression in SELECT: {other}"),
        }
    }
    Ok(Command::Eval(EvalPlan { exprs }))
}

fn parse_scalar_expr(expr: Expr) -> Result<ScalarExpr> {
    match expr {
        Expr::Identifier(ident) => Ok(ScalarExpr::ColumnRef(ident.value)),
        Expr::CompoundIdentifier(parts) => {
            Ok(ScalarExpr::ColumnRef(compound_identifier_to_string(&parts)?))
        }
        Expr::Value(value) => Ok(ScalarExpr::Literal(parse_sql_value(value)?)),
        Expr::BinaryOp { left, op, right } => {
            let arith_op = match op {
                BinaryOperator::Plus => ArithmeticOp::Add,
                BinaryOperator::Minus => ArithmeticOp::Sub,
                BinaryOperator::Multiply => ArithmeticOp::Mul,
                BinaryOperator::Divide => ArithmeticOp::Div,
                BinaryOperator::Modulo => ArithmeticOp::Mod,
                other => bail!("unsupported operator in expression: {other}"),
            };
            Ok(ScalarExpr::BinaryOp {
                left: Box::new(parse_scalar_expr(*left)?),
                op: arith_op,
                right: Box::new(parse_scalar_expr(*right)?),
            })
        }
        Expr::UnaryOp { op, expr } if op.to_string() == "-" => {
            Ok(ScalarExpr::UnaryMinus(Box::new(parse_scalar_expr(*expr)?)))
        }
        Expr::Nested(inner) => parse_scalar_expr(*inner),
        other => bail!("unsupported expression: {other}"),
    }
}
