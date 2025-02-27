use sqlparser::{
    ast::{visit_expressions_mut, visit_relations_mut, Expr, Ident, ObjectName, Value as AstValue},
    dialect::PostgreSqlDialect,
    parser::Parser,
};
use std::ops::ControlFlow;

const PG: &PostgreSqlDialect = &PostgreSqlDialect {};

pub fn preformat(query: &str) -> String {
    let mut ast = Parser::parse_sql(PG, query).unwrap();
    visit_relations_mut(&mut ast, |relation| {
        let ObjectName(idents) = relation;
        for ident in idents {
            modify_table_name(ident);
        }
        ControlFlow::<()>::Continue(())
    });

    visit_expressions_mut(&mut ast, |expr| {
        if let Expr::BinaryOp { left, right, op: _ } = expr {
            modify_binary_op(left);
            modify_binary_op(right);
        }
        if let Expr::CompoundIdentifier(ident) = expr {
            modify_qualified_table_name(ident);
        }
        ControlFlow::<()>::Continue(())
    });
    ast[0].to_string()
}

fn modify_binary_op(ident: &mut Expr) {
    match ident {
        Expr::Value(value) => {
            if let AstValue::SingleQuotedString(s) = value {
                if s.starts_with("0x") {
                    let hex_value = &s[2..];
                    *ident = Expr::Identifier(Ident {
                        value: format!("decode('{}', 'hex')", hex_value),
                        quote_style: None,
                    });
                }
            }
        }
        _ => {}
    }
}

fn modify_qualified_table_name(ident: &mut Vec<Ident>) {
    for ident in ident {
        modify_table_name(ident);
    }
}

fn modify_table_name(ident: &mut Ident) {
    let new_ident = Ident {
        value: ident.value.clone(),
        quote_style: Some('"'),
    };
    *ident = new_ident;
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_preformat() {
        assert_eq!(
            preformat("SELECT column_1 FROM \"table_1\" WHERE column_1 = '123'"),
            "SELECT column_1 FROM \"table_1\" WHERE column_1 = '123'"
        );

        assert_eq!(
            preformat("SELECT column_1 FROM table_1 WHERE column_1 = '123'"),
            "SELECT column_1 FROM \"table_1\" WHERE column_1 = '123'"
        );

        assert_eq!(
            preformat("SELECT \"0xcolumn_1\" FROM table_1 WHERE \"0xcolumn_1\" = '123'"),
            "SELECT \"0xcolumn_1\" FROM \"table_1\" WHERE \"0xcolumn_1\" = '123'"
        );

        assert_eq!(
            preformat("SELECT column_1 FROM table_1 WHERE column_1 = \"0x1234\""),
            "SELECT column_1 FROM \"table_1\" WHERE column_1 = \"0x1234\""
        );

        assert_eq!(
            preformat("SELECT column_1 FROM table_1 WHERE column_1 = '0x1234'"),
            "SELECT column_1 FROM \"table_1\" WHERE column_1 = decode('1234', 'hex')"
        );

        assert_eq!(
            preformat("SELECT column_1 FROM table_1 WHERE '0x1234' = column_1"),
            "SELECT column_1 FROM \"table_1\" WHERE decode('1234', 'hex') = column_1"
        );

        assert_eq!(
            preformat("SELECT store__Tables.tableId FROM store__Tables"),
            "SELECT \"store__Tables\".\"tableId\" FROM \"store__Tables\""
        );

        assert_eq!(
            preformat("SELECT store__Tables.\"tableId\" FROM store__Tables"),
            "SELECT \"store__Tables\".\"tableId\" FROM \"store__Tables\""
        );
    }
}
