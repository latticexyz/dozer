# mud sql

```
SELECT select_list
FROM from_item
WHERE condition
GROUP BY grouping_column_reference [, …]
HAVING group_condition
LIMIT count
OFFSET start
```

where select_list is one of: `*  | [[expression [AS output_name]], …]`

```
*
```

Project all column references for all from_items

```
[[expression [AS output_name]], …]
```

Comma-separated list of value expressions. Value expression can be one of: column reference or aggregate function

If a non-column-reference value expression is used in the select list, it conceptually adds a new virtual column to the returned table. The value expression is evaluated once for each result row, with the row's values substituted for any column references.

If a column name reference is used and if more than one table has a column of the same name, the column name reference must be qualified with the table name. Eg table.column

where aggregate function can be one of:

```
sum()
count()
avg()
max()
min()
sum()
```

where from_item can be one of:

```
[[mud_namespace.mud_table_name [AS name]], …]
```

```
[
	[mud_namespace.mud_table_name [AS name]]
	join_type from_item
	ON join_condition
]
```

where `mud_table_name` is an ascii string representing the name of the MUD table.

where `mud_namespace` is an ascii string representing the namespace of the table. The `mud_namespace` can be omitted in which case the table is assumed to be in the root namespace.

Since each MUD SQL must include a world address in the request, the table is uniquely identified by its namespace and name.

One or more MUD tables. If more than one table is specified then the tables are `CROSS JOIN`ed. A `WHERE` clause can be used to reduce the number of returned rows in the `CROSS JOIN`.

where `join_type` is: `{ [INNER] | { LEFT | RIGHT | FULL } [OUTER] }`

where `join_condition` is a boolean expression

where `condition` is a boolean expression.

Expressions may include scalar sub-queries or table expression sub-queries when combined with the `EXISTS`, `NOT EXISTS`, `IN`, and `NOT IN` operators. Other operators include:

```
^
*
/
%
+
-
BETWEEN
LIKE
ILIKE
<
>
=
<=
>=
<>
IS
IS NULL
IS NOT NULL
NOT
AND
OR
```

where `group_column_reference` is a column name reference

where `group_condition` filters group rows created by `GROUP BY`
