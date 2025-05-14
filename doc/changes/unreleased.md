# Unreleased

This release comes with breaking changes in package `exasol.analytics.schema`:
* Classes `ColumnType` and `ColumnBuilder` are removed
* Class `Column` is changed significantly
* Subclasses of `Column` have been added for specific column types:
  * `BooleanColumn`
  * `CharColumn`
  * `DateColumn`
  * `DecimalColumn`
  * `DoublePrecisionColumn`
  * `GeometryColumn`
  * `HashTypeColumn`
  * `TimeStampColumn`
  * `VarCharColumn`
* Additional classes have been added for specific attributes of some of the column types:
  * `CharSet`
  * `HashSizeUnit`
* Convenience functions for creating instances of `Column` have been replaced by class method `simple()` of the resp. subclasses of `Column`:
  * `decimal_column()`
  * `varchar_column()`
  * `hashtype_column()`

Please see the [User Guide](http://github.com/exasol/advanced-analytics-framework/blob/main/doc/user_guide/database_objects.md) about creating and using instances of `Column` starting with this release.

## Documentation

* #283: Updated description and README
* #290: Added user guide for database objects in module `exasol.analytics.schema`

## Refactorings

* #286: Updated exasol-toolbox to 1.0.1
* #240: Enhanced `schema.column_type.ColumnType`
* #292: Added check for range of attribute `size` to `HashTypeColumn`
* #294: Refactored class `Column` once again, see user guide
  * Re-introduced attribute `type`
  * Re-added class `ColumnType`

## Internal

* #287: Re-locked dependencies to resolve CVE-2025-43859 for h11
* #286: Updated poetry to 2.1.2
