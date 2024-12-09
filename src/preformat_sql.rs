use sqlparser::{
    ast::visit_expressions_mut, ast::Expr, ast::Value as AstValue, dialect::PostgreSqlDialect,
    parser::Parser,
};
use std::ops::ControlFlow;

const PG: &PostgreSqlDialect = &PostgreSqlDialect {};

pub fn preformat(query: &str) -> String {
    let mut ast = Parser::parse_sql(PG, query).unwrap();
    visit_expressions_mut(&mut ast, |expr| {
        if let Expr::BinaryOp {
            left: _,
            right,
            op: _,
        } = expr
        {
            modify_binary_op(right);
        }
        ControlFlow::<()>::Continue(())
    });
    ast[0].to_string()
}

fn modify_binary_op(ident: &mut Expr) {
    match ident {
        Expr::Identifier(ident) => {
            if ident.value.starts_with("0x") {
                let hex_value = &ident.value[2..];
                *ident = sqlparser::ast::Ident {
                    value: format!("decode('{}', 'hex')", hex_value),
                    quote_style: None,
                };
            }
        }
        Expr::Value(value) => {
            if let AstValue::SingleQuotedString(s) = value {
                if s.starts_with("0x") {
                    let hex_value = &s[2..];
                    *ident = sqlparser::ast::Expr::Identifier(sqlparser::ast::Ident {
                        value: format!("decode('{}', 'hex')", hex_value),
                        quote_style: None,
                    });
                }
            }
        }
        _ => {}
    }
}
