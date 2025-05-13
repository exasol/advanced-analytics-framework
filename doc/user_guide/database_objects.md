# Database Objects

The Advanced Analytics Framework (AAF) contains Python module `exasol.analytics.schema` for convenient handling database objects.

The module contains interfaces and implementations:

* Class `Table` describes an SQL TABLE
* Class `View` describes an SQL VIEW
* Class `Column` describes columns of SQL tables and views, see [Class Column](#class-column)

Interfaces and implementation classes for names of specific Database objects:

| Interface class        | Implementation class       |
|------------------------|----------------------------|
| `SchemaName`           | -                          |
| `ColumnName`           | -                          |
| `TableName`            | `TableNameImpl`            |
| `ConnectionObjectName` | `ConnectionObjectNameImpl` |
| `UDFName`              | `UDFNameImpl`              |
| `ViewName`             | `ViewNameImpl`             |


## Class Column

### Subclasses for each of the SQL column types

There is a subclass with specific attributes for each of the SQL types:

| Class                   | Attributes, value range, default value |
|-------------------------|------------------------------------------------------------------------|
| `BooleanColumn`         | -                                                                      |
| `CharColumn`            | `size` (0-2000, default: 1), `charset`: CharSet, default: CharSet.UTF8 |
| `DateColumn`            | -                                                                      |
| `DecimalColumn`         | `precision` (0-37, default: 18), `scale` (0-37, default: 0)            |
| `DoublePrecisionColumn` | -                                                                      |
| `GeometryColumn`        | `srid`, (default: 0)                                                   |
| `HashTypeColumn`        | `size` (default: 16), `unit` (`HashSizeUnit.BYTE` or `HashSizeUnit.BIT`, default: `HashSizeUnit.BYTE`) |
| `TimeStampColumn`       | `precision` (1-9, default: 3), `local_time_zone` (`True` or `False`, default: False)      |
| `VarCharColumn`         | `size` (0-2000000), `charset`: CharSet (`CharSet.UTF8` or `CharSet.ASCII`, default: `CharSet.UTF8`) |

### Instanciate a Column Object

You can instantiate a column:
```python
col = DecimalColumn(ColumnName("D"), precision=10, scale=1)
```

For convenience there is also a classmethod simple() accepting a simple string for the column name:
```python
col = DecimalColumn.simple("D", precision=10, scale=1)
```

### Render for a `CREATE TABLE` statement

Each column can be rendered for creating a `CREATE TABLE` statement:
```python
DecimalColumn.simple("D", precision=10, scale=1).for_create
>>> DECIMAL "D"(18,1)
```

### Parse from SQL specification

Each column can be parsed from its SQL specification:
```python
char_column = Column.from_sql_spec("C", "CHAR(1) UTF8")
```

This conversion supports aliases
* `INTEGER`, `DECIMAL` for `DecimalColumn`
* `DOUBLE PRECISION`, `DOUBLE`, `FLOAT` for `DoublePrecisionColumn`

### Parse from Pyexasol column metadata

Each column can be parsed from [Pyexasol](https://github.com/exasol/pyexasol) column metadata:
```python
timestamp_column = Column.from_pyexasol("A", {"type": "TIMESTAMP", "withLocalTimeZone": True})
```
