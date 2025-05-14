# Database Objects

The Advanced Analytics Framework (AAF) contains Python module `exasol.analytics.schema` for conveniently handling database objects.

The module contains interfaces and implementations:

* Class `Table` describes a SQL TABLE
* Class `View` describes a SQL VIEW
* Class `Column` describes columns of SQL tables and views, see [Class Column](#class-column)

Interfaces and implementation classes for names of specific database objects:

| Interface class        | Implementation class       |
|------------------------|----------------------------|
| `SchemaName`           | -                          |
| `ColumnName`           | -                          |
| `TableName`            | `TableNameImpl`            |
| `ConnectionObjectName` | `ConnectionObjectNameImpl` |
| `UDFName`              | `UDFNameImpl`              |
| `ViewName`             | `ViewNameImpl`             |

## Class `Column`

Class `Column` has attributes
* `name` of type `ColumnName`
* `type` of type `ColumnType`

Additionally there are convenience methods for
* Rendering the column for a `CREATE TABLE` statement
* Creating a new instance by parsing its SQL specification

## Class `ColumnType`

### Subclasses for Each of the SQL Column Types

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

### Instantiating a `Column` Object

You can instantiate a column:

```python
col = Column(ColumnName("D"), DecimalColumn(precision=10, scale=1))
```

For convenience, rach of the subclasses of `ColumnType` also provides a class method `simple()`, which accepts a simple string for the column name:

```python
col = DecimalColumn.simple("D", precision=10, scale=1)
```

### Rendering for a `CREATE TABLE` Statement

Each column can be rendered for creating a `CREATE TABLE` statement:
```python
DecimalColumn.simple("D", precision=10, scale=1).for_create
>>> "D" DECIMAL(18,1)
```

### Parse from SQL Specification

A column including its name and type can be parsed from its SQL specification:

```python
char_column = Column.from_sql_spec("C", "CHAR(1) UTF8")
```

This conversion supports aliases for some column types:
* `INTEGER`, `DECIMAL` for `DecimalColumn`
* `DOUBLE PRECISION`, `DOUBLE`, `FLOAT` for `DoublePrecisionColumn`

### Parsing from Pyexasol Column Metadata

A column can also be parsed from [Pyexasol](https://github.com/exasol/pyexasol) column metadata:

```python
timestamp_column = Column.from_pyexasol("A", {"type": "TIMESTAMP", "withLocalTimeZone": True})
```
