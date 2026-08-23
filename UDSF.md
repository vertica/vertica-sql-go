# User-Defined SQL Function (UDSF) Support in Vertica Go Driver

## Overview

The Vertica Go driver now provides comprehensive support for User-Defined SQL Function (UDSF) lifecycle operations. This includes:

- **CREATE FUNCTION**: Define new user-defined SQL functions
- **ALTER FUNCTION**: Modify existing function properties
- **DROP FUNCTION**: Remove functions from the database
- **GRANT/REVOKE**: Manage privilege access to functions and schemas
- **Function Invocation**: Call user-defined functions within SQL queries
- **Cross-RDBMS Function Emulation**: Define SQL functions that emulate built-in functions from other RDBMS platforms

## Architecture

### Core Components

#### 1. UDSF Analyzer (`udsf.go`)

The `UDSFAnalyzer` is the central component that:

- **Detects UDSF Statements**: Identifies whether a SQL statement is a UDSF-related operation
- **Tokenizes Safely**: Performs SQL tokenization without modifying or interpreting statement content
- **Respects SQL Syntax**: Handles comments, string literals, quoted identifiers, and dollar-quoted strings
- **Treats as Atomic Units**: Marks UDSF statements to prevent statement splitting

**Key Functions:**

```go
// Determine if a statement is UDSF-related
func (ua *UDSFAnalyzer) IsUDSFStatement(sql string) bool

// Get the specific type of UDSF statement
func (ua *UDSFAnalyzer) GetStatementType(sql string) (UDSFStatementType, error)

// Determine if statement should not be split
func (ua *UDSFAnalyzer) ShouldTreatAsAtomicUnit(sql string) bool
```

**Statement Types:**

```go
UDSFStatementTypeCreateFunction  // CREATE FUNCTION
UDSFStatementTypeAlterFunction   // ALTER FUNCTION
UDSFStatementTypeDropFunction    // DROP FUNCTION
UDSFStatementTypeGrantUsage      // GRANT USAGE ON SCHEMA
UDSFStatementTypeGrantExecute    // GRANT EXECUTE ON FUNCTION
UDSFStatementTypeRevokeUsage     // REVOKE USAGE ON SCHEMA
UDSFStatementTypeRevokeExecute   // REVOKE EXECUTE ON FUNCTION
```

#### 2. Statement Handler Integration (`stmt.go`)

Modifications to `stmt.go` ensure UDSF statements bypass the prepared statement splitting mechanism:

- **Early Detection in `newStmt()`**: UDSF statements are identified before lexical analysis
- **Simple Query Protocol**: UDSF statements use the simple query protocol (like LOCAL COPY) to preserve integrity
- **No Client-Side Parsing**: Function bodies are never parsed or modified by the driver

**Key Integration Point:**

```go
// In newStmt() - UDSF statements are identified and marked as non-splittable
analyzer := NewUDSFAnalyzer()
if analyzer.IsUDSFStatement(command) {
    s.multiStatements = false  // Prevent splitting
    return s, nil
}

// In isLocalCopyStatement() - UDSF statements use simple query protocol
if analyzer.IsUDSFStatement(statements[0]) {
    return true  // Use simple protocol
}
```

### Design Principles

1. **Transparent Pass-Through**: The driver does not interpret, parse, or modify SQL content
2. **Lexical Tokenization Only**: Statement type detection uses safe tokenization without regex
3. **Atomic Execution**: UDSF statements are never split into multiple statements
4. **Error Transparency**: Server errors are returned unchanged to the application
5. **Backward Compatibility**: All existing SQL operations continue to work unchanged

### SQL Tokenization Strategy

The tokenizer respects all SQL syntax elements:

- **String Literals**: Single-quoted strings with doubled-quote escaping
- **Quoted Identifiers**: Double-quoted identifiers with doubled-quote escaping
- **Dollar-Quoted Strings**: `$tag$...$tag$` syntax for function bodies
- **Comments**: Line comments (`--`) and block comments (`/* */`)
- **Case-Insensitivity**: SQL keywords are matched case-insensitively

## Usage Examples

### Creating Simple Functions

```go
func main() {
    db, _ := sql.Open("vertica-sql-go", connStr)
    defer db.Close()

    // Create a simple SQL function
    createSQL := `CREATE FUNCTION add_one(x INT)
    RETURNS INT AS
    'SELECT x + 1'
    LANGUAGE SQL`
    
    _, err := db.ExecContext(ctx, createSQL)
    if err != nil {
        log.Fatal(err)
    }

    // Invoke the function
    var result int
    err = db.QueryRowContext(ctx, `SELECT add_one(5)`).Scan(&result)
    fmt.Println("Result:", result) // Output: 6
}
```

### Creating Complex Functions with Logic

```go
createSQL := `CREATE FUNCTION categorize_value(val INT)
RETURNS VARCHAR AS
$$
BEGIN
  RETURN CASE
    WHEN val < 0 THEN 'negative'
    WHEN val = 0 THEN 'zero'
    ELSE 'positive'
  END;
END;
$$
LANGUAGE PLPGSQL`

_, err := db.ExecContext(ctx, createSQL)
```

### Handling Embedded Semicolons

```go
// Semicolons inside string literals are preserved
createSQL := `CREATE FUNCTION format_sql()
RETURNS VARCHAR AS
'SELECT ''SELECT 1; SELECT 2; SELECT 3;'''
LANGUAGE SQL`

_, err := db.ExecContext(ctx, createSQL)
```

### Dollar-Quoted Function Bodies

```go
// Dollar-quoting allows arbitrary SQL content
createSQL := `CREATE FUNCTION process_data(id INT)
RETURNS INT AS
$func$
DECLARE
  result INT := 0;
BEGIN
  -- Complex logic here with any syntax
  SELECT COUNT(*) INTO result FROM data WHERE id = id;
  RETURN result;
END;
$func$
LANGUAGE PLPGSQL`

_, err := db.ExecContext(ctx, createSQL)
```

### Privilege Management

```go
// Grant EXECUTE privilege on function to a role
grantSQL := `GRANT EXECUTE ON FUNCTION add_one(INT) TO analyst_role`
_, err := db.ExecContext(ctx, grantSQL)

// Grant USAGE privilege on schema to enable function access
usageSQL := `GRANT USAGE ON SCHEMA public TO analyst_role`
_, err := db.ExecContext(ctx, usageSQL)

// Revoke privileges
revokeSQL := `REVOKE EXECUTE ON FUNCTION add_one(INT) FROM analyst_role`
_, err := db.ExecContext(ctx, revokeSQL)
```

### Multi-Parameter Functions

```go
createSQL := `CREATE FUNCTION calculate(a INT, b INT, op VARCHAR)
RETURNS INT AS
$$
SELECT CASE op
  WHEN 'ADD' THEN a + b
  WHEN 'SUB' THEN a - b
  WHEN 'MUL' THEN a * b
  ELSE 0
END;
$$
LANGUAGE SQL`

_, err := db.ExecContext(ctx, createSQL)

// Invoke with parameters
var result int
err = db.QueryRowContext(ctx, `SELECT calculate(10, 5, 'ADD')`).Scan(&result)
```

### Altering Functions

```go
// Modify function properties
alterSQL := `ALTER FUNCTION add_one(INT) IMMUTABLE`
_, err := db.ExecContext(ctx, alterSQL)
```

### Dropping Functions

```go
// Drop function
dropSQL := `DROP FUNCTION IF EXISTS add_one(INT) CASCADE`
_, err := db.ExecContext(ctx, dropSQL)
```

### Function Invocation in Complex Queries

```go
// Use function in SELECT clauses
rows, err := db.QueryContext(ctx, 
    `SELECT id, name, add_one(score) AS adjusted_score FROM results`)

// Use function in WHERE clauses
rows, err := db.QueryContext(ctx,
    `SELECT * FROM data WHERE categorize_value(amount) = 'positive'`)

// Use function in INSERT statements
_, err := db.ExecContext(ctx,
    `INSERT INTO audit_log (old_val, new_val) VALUES (calculate(5, 3, 'ADD'), $1)`, value)
```

## Behavior Guarantees

### Correct Handling

✅ **Multi-line Function Bodies**: Preserved exactly as written  
✅ **Embedded Semicolons**: Only treated as separators outside quotes/comments  
✅ **Comments**: Line comments (`--`) and block comments (`/* */`) are ignored during tokenization  
✅ **String Literals**: Single quotes, double quotes, and dollar quotes are respected  
✅ **Quoted Identifiers**: Mixed-case identifiers in quotes are preserved  
✅ **CASE Logic**: Complex CASE expressions with multiple branches work correctly  
✅ **BEGIN...END Blocks**: Multi-statement blocks are preserved  
✅ **Null Handling**: NULL values in function parameters and returns are handled correctly  
✅ **Transactions**: UDSF operations work correctly within explicit transactions  
✅ **Error Messages**: Vertica server errors are returned unchanged  

### No Client-Side Interpretation

❌ **Function Body Parsing**: Driver never parses or interprets function bodies  
❌ **SQL Rewriting**: SQL content is never rewritten or reformatted  
❌ **Statement Splitting**: Function bodies are never split into multiple statements  
❌ **Content Modification**: String content is never modified  
❌ **Regex Patterns**: No regex patterns are used for statement detection (as requested)  

## Performance Considerations

1. **Minimal Overhead**: UDSF detection adds only O(n) tokenization overhead on statement creation
2. **No Caching Overhead**: Statement types are determined once per execution
3. **Simple Protocol**: UDSF statements use the simple query protocol (same as LOCAL COPY)
4. **Memory Efficient**: Tokenization uses streaming analysis without buffering the entire statement

## Compatibility

- **Vertica Server**: Works with Vertica 9.3+ (requires UDF support on server)
- **SQL Dialect**: Supports both SQL and PL/pgSQL function definitions
- **Cross-RDBMS**: Functions can be written to emulate built-in functions from:
  - PostgreSQL
  - Oracle
  - MySQL
  - SQL Server
  - Other RDBMS platforms

## Error Handling

The driver follows strict error transparency:

```go
// Example: Invalid function syntax
createSQL := `CREATE FUNCTION bad_func()
RETURNS INT AS
'SELECT * FROM nonexistent_table'
LANGUAGE SQL`

_, err := db.ExecContext(ctx, createSQL)
if err != nil {
    // err contains the exact Vertica server error message
    // Examples:
    // - "ERROR: table nonexistent_table does not exist"
    // - "ERROR: syntax error in function body"
    // NOT driver parsing errors
}
```

## Testing Strategy

The implementation includes:

1. **Unit Tests (`udsf_test.go`)**: 
   - 20+ tests covering UDSF detection
   - Tokenization tests with edge cases
   - Comment and string literal handling
   - Case sensitivity and quoted identifiers

2. **Integration Tests (`udsf_integration_test.go`)**:
   - CREATE FUNCTION with various function body types
   - ALTER FUNCTION operations
   - DROP FUNCTION cleanup
   - GRANT/REVOKE privilege management
   - Function invocation in SELECT, INSERT, UPDATE, DELETE
   - NULL handling and error conditions
   - Transaction handling
   - Regression tests for existing SQL operations

## Future Enhancements

1. **Function Overloading**: Support for multiple function signatures with same name
2. **Default Parameters**: Functions with default parameter values
3. **OUT Parameters**: Functions returning multiple values via OUT parameters
4. **Aggregate Functions**: User-defined aggregate functions
5. **Type Conversion**: Functions with complex parameter type mappings

## Troubleshooting

### Issue: "parse error" when creating function

**Cause**: Likely a temporary regression in the driver  
**Solution**: Check if UDSF detection correctly identified the statement type

```go
analyzer := NewUDSFAnalyzer()
isUDSF := analyzer.IsUDSFStatement(yourSQL)
fmt.Println("Is UDSF:", isUDSF)
```

### Issue: Function not found after creation

**Cause**: Function was created in a different schema or with different parameter types  
**Solution**: Verify function name and parameters match exactly when calling

```go
// Check what functions exist
rows, _ := db.QueryContext(ctx, 
    `SELECT function_name, parameters FROM user_functions ORDER BY 1`)
```

### Issue: Permission denied when calling function

**Cause**: Caller lacks EXECUTE privilege or USAGE privilege on schema  
**Solution**: Grant appropriate privileges as database superuser

```go
// Grant USAGE on schema
db.ExecContext(ctx, `GRANT USAGE ON SCHEMA myschema TO myuser`)

// Grant EXECUTE on function
db.ExecContext(ctx, `GRANT EXECUTE ON FUNCTION myfunc(INT) TO myuser`)
```

## References

- Vertica Documentation: User-Defined SQL Functions
- PostgreSQL Documentation: CREATE FUNCTION
- Go database/sql Package: https://golang.org/pkg/database/sql/
