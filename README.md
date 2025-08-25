# dbqcore

DataBridge Quality Core library is a part of [dbqctl](https://github.com/DataBridgeTech/dbqctl).

## Available check functions:
- Schema:
  - `expect_columns_ordered`: Validate table columns match an ordered list
  - `expect_columns`: Validate table has one of columns from unordered list
  - `columns_not_present`: Validate table doesn't have any columns from the list or matching pattern
- Table:
  - `row_count`: Count of rows in the table
  - `raw_query`: Custom SQL query for complex validations
- Column:
  - `not_null`: Check for null values in a column
  - `freshness`: Check data recency based on timestamp column
  - `uniqueness`: Check for unique values in a column
  - `min`/max: Minimum and maximum values for numeric columns
  - `sum`: Sum of values in a column
  - `avg`: Average of values in a column
  - `stddev`: Standard deviation of values in a column

### Operators supported:
  - Comparison: `<, >, <=, >=, ==, !=`
  - Range: `between X and Y`
  - Function-only checks (like `not_null, uniqueness`)

## Changelog

### v0.5.0

#### Added
- **Schema Validation**: New `schema_checks` support for validating database table schemas
    - `expect_columns` check to validate required column presence and types
    - `expect_columns_ordered` check to validate required column presence and types in specific order
    - `columns_not_present` check to ensure specific columns are not present by stop-list or pattern
- **Enhanced Check Configuration**: New flexible checks format with improved YAML configuration
- **Database Adapter Architecture**: Refactored to use adapter pattern for better database abstraction
- **Comprehensive Test Coverage**: Added extensive test suites for all adapters and validation logic
- **CI/CD Pipeline**: GitHub Actions workflow for automated testing

#### Improved
- **Performance**: Enhanced query execution with optimized adapter interfaces
- **Configuration**: More flexible check expression parsing and validation
- **Error Handling**: Better validation and error reporting for check results
- **Code Quality**: Comprehensive refactoring with improved maintainability