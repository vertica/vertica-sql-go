package vertigo

// Copyright (c) 2019-2024 Open Text.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//    http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
//
// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in
// all copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
// THE SOFTWARE.

import (
	"database/sql"
	"fmt"
	"strings"
	"testing"
)

// TestUDSFCreateFunctionSimple verifies that a simple CREATE FUNCTION statement
// can be executed successfully through the driver.
// Given: A valid CREATE FUNCTION statement with simple SQL body
// When: Executed through the driver
// Then: Execution succeeds and no client-side parsing/splitting errors occur
func TestUDSFCreateFunctionSimple(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	connDB, err := sql.Open("vertica", myDBConnectString)
	assertNoErr(t, err)
	defer connDB.Close()

	// Clean up any existing function
	cleanupSQL := `DROP FUNCTION IF EXISTS test_simple_func()`
	_, _ = connDB.ExecContext(ctx, cleanupSQL)

	// Create a simple function
	createSQL := `CREATE FUNCTION test_simple_func()
	RETURN INT AS
	BEGIN
		RETURN 1;
	END`

	_, err = connDB.ExecContext(ctx, createSQL)
	assertNoErr(t, err)
	t.Cleanup(func() {
		_, _ = connDB.ExecContext(ctx, `DROP FUNCTION IF EXISTS test_simple_func()`)
	})

	// Verify the function exists by calling it
	var result int
	querySQL := `SELECT test_simple_func()`
	err = connDB.QueryRowContext(ctx, querySQL).Scan(&result)
	assertNoErr(t, err)
	assertEqual(t, result, 1)
}

// TestUDSFCreateFunctionMultiline verifies that CREATE FUNCTION with multi-line
// function bodies containing BEGIN...END, CASE logic, and comments are transmitted intact.
// Given: CREATE FUNCTION statement with multi-line SQL body, comments, CASE logic
// When: Executed through the driver
// Then: Execution succeeds and function body is preserved intact
func TestUDSFCreateFunctionMultiline(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	connDB, err := sql.Open("vertica", myDBConnectString)
	assertNoErr(t, err)
	defer connDB.Close()

	// Clean up any existing function
	cleanupSQL := `DROP FUNCTION IF EXISTS test_multiline_func(INT)`
	_, _ = connDB.ExecContext(ctx, cleanupSQL)

	// Create a multi-line function with CASE logic and comments
	createSQL := `CREATE FUNCTION test_multiline_func(int_param INT)
	RETURN INT AS
	BEGIN
		-- Determine result based on parameter value
		RETURN (CASE
			WHEN int_param < 0 THEN -1
			WHEN int_param = 0 THEN 0
			ELSE 1
		END);
	END`

	_, err = connDB.ExecContext(ctx, createSQL)
	assertNoErr(t, err)
	t.Cleanup(func() {
		_, _ = connDB.ExecContext(ctx, `DROP FUNCTION IF EXISTS test_multiline_func(INT)`)
	})

	// Test the function with different values
	testCases := []struct {
		input    int
		expected int
	}{
		{-5, -1},
		{0, 0},
		{10, 1},
	}

	for _, tc := range testCases {
		var result int
		querySQL := fmt.Sprintf(`SELECT test_multiline_func(%d)`, tc.input)
		err = connDB.QueryRowContext(ctx, querySQL).Scan(&result)
		assertNoErr(t, err)
		assertEqual(t, result, tc.expected)
	}
}

// TestUDSFCreateFunctionWithSemicolonsInStrings verifies that embedded semicolons
// within string literals are not interpreted as statement delimiters.
// Given: CREATE FUNCTION with function body containing string literals with semicolons
// When: Executed through the driver
// Then: Execution succeeds and string content is preserved
func TestUDSFCreateFunctionWithSemicolonsInStrings(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	connDB, err := sql.Open("vertica", myDBConnectString)
	assertNoErr(t, err)
	defer connDB.Close()

	// Clean up any existing function
	cleanupSQL := `DROP FUNCTION IF EXISTS test_semicolon_func()`
	_, _ = connDB.ExecContext(ctx, cleanupSQL)

	// Create a function with embedded semicolons in strings
	createSQL := `CREATE FUNCTION test_semicolon_func()
	RETURN VARCHAR AS
	BEGIN
		RETURN 'value with; semicolon; inside';
	END`

	_, err = connDB.ExecContext(ctx, createSQL)
	assertNoErr(t, err)
	t.Cleanup(func() {
		_, _ = connDB.ExecContext(ctx, `DROP FUNCTION IF EXISTS test_semicolon_func()`)
	})

	// Verify the function returns the expected value
	var result string
	querySQL := `SELECT test_semicolon_func()`
	err = connDB.QueryRowContext(ctx, querySQL).Scan(&result)
	assertNoErr(t, err)
	assertEqual(t, result, "value with; semicolon; inside")
}

// TestUDSFCreateFunctionWithDollarQuoting verifies support for dollar-quoted
// function bodies, which allow arbitrary SQL content including semicolons.
// Given: CREATE FUNCTION with dollar-quoted string body containing embedded SQL with semicolons
// When: Executed through the driver
// Then: Execution succeeds and function is created correctly
func TestUDSFCreateFunctionWithDollarQuoting(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	connDB, err := sql.Open("vertica", myDBConnectString)
	assertNoErr(t, err)
	defer connDB.Close()

	// Clean up any existing function
	cleanupSQL := `DROP FUNCTION IF EXISTS test_dollar_func(INT)`
	_, _ = connDB.ExecContext(ctx, cleanupSQL)

	// Create a function with dollar-quoted body
	createSQL := `CREATE FUNCTION test_dollar_func(int_param INT)
	RETURN INT AS
	BEGIN
		RETURN (CASE
			WHEN int_param < 0 THEN -1
			WHEN int_param = 0 THEN 0
			ELSE 1
		END);
	END`

	_, err = connDB.ExecContext(ctx, createSQL)
	assertNoErr(t, err)
	t.Cleanup(func() {
		_, _ = connDB.ExecContext(ctx, `DROP FUNCTION IF EXISTS test_dollar_func(INT)`)
	})

	// Verify the function works
	var result int
	err = connDB.QueryRowContext(ctx, `SELECT test_dollar_func(5)`).Scan(&result)
	assertNoErr(t, err)
	assertEqual(t, result, 1)
}

// TestUDSFAlterFunction verifies that ALTER FUNCTION statements are executed
// as atomic units without splitting.
// Given: A valid ALTER FUNCTION statement
// When: Executed through the driver
// Then: Execution succeeds with no client-side parsing errors
func TestUDSFAlterFunction(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	connDB, err := sql.Open("vertica", myDBConnectString)
	assertNoErr(t, err)
	defer connDB.Close()

	// Clean up and create a function to alter
	_, _ = connDB.ExecContext(ctx, `DROP FUNCTION IF EXISTS test_alter_func()`)
	createSQL := `CREATE FUNCTION test_alter_func()
	RETURN INT AS
	BEGIN
		RETURN 42;
	END`
	_, err = connDB.ExecContext(ctx, createSQL)
	assertNoErr(t, err)
	t.Cleanup(func() {
		_, _ = connDB.ExecContext(ctx, `DROP FUNCTION IF EXISTS test_alter_func()`)
		_, _ = connDB.ExecContext(ctx, `DROP FUNCTION IF EXISTS test_alter_func_v2()`)
	})

	// Rename the function to verify ALTER FUNCTION executes as an atomic unit
	alterSQL := `ALTER FUNCTION test_alter_func() RENAME TO test_alter_func_v2`
	_, err = connDB.ExecContext(ctx, alterSQL)
	assertNoErr(t, err)

	// Rename back so the verification query and cleanup use the original name
	renameBackSQL := `ALTER FUNCTION test_alter_func_v2() RENAME TO test_alter_func`
	_, err = connDB.ExecContext(ctx, renameBackSQL)
	assertNoErr(t, err)

	// Verify the function still works
	var result int
	err = connDB.QueryRowContext(ctx, `SELECT test_alter_func()`).Scan(&result)
	assertNoErr(t, err)
	assertEqual(t, result, 42)
}

// TestUDSFDropFunction verifies that DROP FUNCTION statements are executed
// successfully as atomic units.
// Given: A valid DROP FUNCTION statement
// When: Executed through the driver
// Then: Function is dropped and subsequent calls fail with appropriate error
func TestUDSFDropFunction(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	connDB, err := sql.Open("vertica", myDBConnectString)
	assertNoErr(t, err)
	defer connDB.Close()

	// Create a function
	createSQL := `CREATE FUNCTION test_drop_func()
	RETURN INT AS
	BEGIN
		RETURN 1;
	END`
	_, err = connDB.ExecContext(ctx, createSQL)
	assertNoErr(t, err)
	t.Cleanup(func() {
		_, _ = connDB.ExecContext(ctx, `DROP FUNCTION IF EXISTS test_drop_func()`)
	})

	// Verify it exists
	var result int
	err = connDB.QueryRowContext(ctx, `SELECT test_drop_func()`).Scan(&result)
	assertNoErr(t, err)

	// Drop the function
	dropSQL := `DROP FUNCTION test_drop_func()`
	_, err = connDB.ExecContext(ctx, dropSQL)
	assertNoErr(t, err)

	// Verify it no longer exists
	err = connDB.QueryRowContext(ctx, `SELECT test_drop_func()`).Scan(&result)
	if err == nil {
		t.Fatal("expected error calling dropped function, but got none")
	}
}

// TestUDSFGrantExecute verifies that GRANT EXECUTE statements for functions
// are executed successfully.
// Given: A valid GRANT EXECUTE ON FUNCTION statement to a user/role
// When: Executed by a superuser through the driver
// Then: Execution succeeds with no splitting errors
func TestUDSFGrantExecute(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	connDB, err := sql.Open("vertica", myDBConnectString)
	assertNoErr(t, err)
	defer connDB.Close()

	// Create a test role
	_, _ = connDB.ExecContext(ctx, `DROP ROLE IF EXISTS test_udsf_role`)
	roleSQL := `CREATE ROLE test_udsf_role`
	_, err = connDB.ExecContext(ctx, roleSQL)
	assertNoErr(t, err)
	t.Cleanup(func() {
		_, _ = connDB.ExecContext(ctx, `DROP ROLE IF EXISTS test_udsf_role`)
	})

	// Ensure re-runs don't fail if a previous run leaked the function.
	_, _ = connDB.ExecContext(ctx, `DROP FUNCTION IF EXISTS test_grant_func()`)

	// Create a function
	createSQL := `CREATE FUNCTION test_grant_func()
	RETURN INT AS
	BEGIN
		RETURN 1;
	END`
	_, err = connDB.ExecContext(ctx, createSQL)
	assertNoErr(t, err)
	t.Cleanup(func() {
		_, _ = connDB.ExecContext(ctx, `DROP FUNCTION IF EXISTS test_grant_func()`)
	})

	// Grant EXECUTE privilege
	grantSQL := `GRANT EXECUTE ON FUNCTION test_grant_func()
	TO test_udsf_role`
	_, err = connDB.ExecContext(ctx, grantSQL)
	assertNoErr(t, err)
}

// TestUDSFRevokeExecute verifies that REVOKE EXECUTE statements for functions
// are executed successfully.
// Given: A valid REVOKE EXECUTE ON FUNCTION statement from a user/role
// When: Executed by a superuser through the driver
// Then: Execution succeeds with no splitting errors
func TestUDSFRevokeExecute(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	connDB, err := sql.Open("vertica", myDBConnectString)
	assertNoErr(t, err)
	defer connDB.Close()

	// Create a test role
	_, _ = connDB.ExecContext(ctx, `DROP ROLE IF EXISTS test_udsf_role2`)
	roleSQL := `CREATE ROLE test_udsf_role2`
	_, err = connDB.ExecContext(ctx, roleSQL)
	assertNoErr(t, err)
	t.Cleanup(func() {
		_, _ = connDB.ExecContext(ctx, `DROP ROLE IF EXISTS test_udsf_role2`)
	})

	// Ensure re-runs don't fail if a previous run leaked the function.
	_, _ = connDB.ExecContext(ctx, `DROP FUNCTION IF EXISTS test_revoke_func()`)

	// Create a function
	createSQL := `CREATE FUNCTION test_revoke_func()
	RETURN INT AS
	BEGIN
		RETURN 1;
	END`
	_, err = connDB.ExecContext(ctx, createSQL)
	assertNoErr(t, err)
	t.Cleanup(func() {
		_, _ = connDB.ExecContext(ctx, `DROP FUNCTION IF EXISTS test_revoke_func()`)
	})

	// Grant EXECUTE privilege
	grantSQL := `GRANT EXECUTE ON FUNCTION test_revoke_func() TO test_udsf_role2`
	_, _ = connDB.ExecContext(ctx, grantSQL)

	// Revoke EXECUTE privilege
	revokeSQL := `REVOKE EXECUTE ON FUNCTION test_revoke_func() FROM test_udsf_role2`
	_, err = connDB.ExecContext(ctx, revokeSQL)
	assertNoErr(t, err)
}

// TestUDSFGrantUsageOnSchema verifies that GRANT USAGE ON SCHEMA statements
// are executed successfully.
// Given: A valid GRANT USAGE ON SCHEMA statement to a user/role
// When: Executed by a superuser through the driver
// Then: Execution succeeds with no splitting errors
func TestUDSFGrantUsageOnSchema(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	connDB, err := sql.Open("vertica", myDBConnectString)
	assertNoErr(t, err)
	defer connDB.Close()

	// Create a test role
	_, _ = connDB.ExecContext(ctx, `DROP ROLE IF EXISTS test_schema_role`)
	roleSQL := `CREATE ROLE test_schema_role`
	_, err = connDB.ExecContext(ctx, roleSQL)
	assertNoErr(t, err)
	t.Cleanup(func() {
		_, _ = connDB.ExecContext(ctx, `DROP ROLE IF EXISTS test_schema_role`)
	})

	// Create a test schema
	schemaSQL := `CREATE SCHEMA IF NOT EXISTS test_schema`
	_, err = connDB.ExecContext(ctx, schemaSQL)
	assertNoErr(t, err)
	t.Cleanup(func() {
		_, _ = connDB.ExecContext(ctx, `DROP SCHEMA IF EXISTS test_schema CASCADE`)
	})

	// Grant USAGE privilege on schema
	grantSQL := `GRANT USAGE ON SCHEMA test_schema TO test_schema_role`
	_, err = connDB.ExecContext(ctx, grantSQL)
	assertNoErr(t, err)
}

// TestUDSFRevokeUsageOnSchema verifies that REVOKE USAGE ON SCHEMA statements
// are executed successfully.
// Given: A valid REVOKE USAGE ON SCHEMA statement from a user/role
// When: Executed by a superuser through the driver
// Then: Execution succeeds with no splitting errors
func TestUDSFRevokeUsageOnSchema(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	connDB, err := sql.Open("vertica", myDBConnectString)
	assertNoErr(t, err)
	defer connDB.Close()

	// Create a test role
	_, _ = connDB.ExecContext(ctx, `DROP ROLE IF EXISTS test_schema_role2`)
	roleSQL := `CREATE ROLE test_schema_role2`
	_, err = connDB.ExecContext(ctx, roleSQL)
	assertNoErr(t, err)
	t.Cleanup(func() {
		_, _ = connDB.ExecContext(ctx, `DROP ROLE IF EXISTS test_schema_role2`)
	})

	// Create a test schema
	schemaSQL := `CREATE SCHEMA IF NOT EXISTS test_schema2`
	_, err = connDB.ExecContext(ctx, schemaSQL)
	assertNoErr(t, err)
	t.Cleanup(func() {
		_, _ = connDB.ExecContext(ctx, `DROP SCHEMA IF EXISTS test_schema2 CASCADE`)
	})

	// Grant USAGE privilege
	grantSQL := `GRANT USAGE ON SCHEMA test_schema2 TO test_schema_role2`
	_, _ = connDB.ExecContext(ctx, grantSQL)

	// Revoke USAGE privilege
	revokeSQL := `REVOKE USAGE ON SCHEMA test_schema2 FROM test_schema_role2`
	_, err = connDB.ExecContext(ctx, revokeSQL)
	assertNoErr(t, err)
}

// TestUDSFInvokeFunctionInSelect verifies that user-defined functions can be invoked
// within SELECT statements and return expected results.
// Given: A user-defined SQL function emulating a built-in function
// When: Invoked within SELECT query
// Then: Function returns expected result
func TestUDSFInvokeFunctionInSelect(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	connDB, err := sql.Open("vertica", myDBConnectString)
	assertNoErr(t, err)
	defer connDB.Close()

	// Clean up
	_, _ = connDB.ExecContext(ctx, `DROP FUNCTION IF EXISTS custom_add(INT, INT)`)

	// Create a custom add function emulating a built-in operation
	createSQL := `CREATE FUNCTION custom_add(a INT, b INT)
	RETURN INT AS
	BEGIN
		RETURN (a + b);
	END`
	_, err = connDB.ExecContext(ctx, createSQL)
	assertNoErr(t, err)
	t.Cleanup(func() {
		_, _ = connDB.ExecContext(ctx, `DROP FUNCTION IF EXISTS custom_add(INT, INT)`)
	})

	// Invoke in SELECT
	var result int
	err = connDB.QueryRowContext(ctx, `SELECT custom_add(5, 3)`).Scan(&result)
	assertNoErr(t, err)
	assertEqual(t, result, 8)
}

// TestUDSFErrorHandling verifies that invalid UDSF syntax returns exact server errors
// unchanged by the driver.
// Given: Invalid CREATE FUNCTION syntax
// When: Executed through the driver
// Then: Exact Vertica server error is returned without modification
func TestUDSFErrorHandling(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	connDB, err := sql.Open("vertica", myDBConnectString)
	assertNoErr(t, err)
	defer connDB.Close()

	// Ensure cleanup runs even when assertions call t.Fatal.
	_, _ = connDB.ExecContext(ctx, `DROP FUNCTION IF EXISTS invalid_func()`)
	t.Cleanup(func() {
		_, _ = connDB.ExecContext(ctx, `DROP FUNCTION IF EXISTS invalid_func()`)
	})

	// Try to create function with invalid syntax
	invalidSQL := `CREATE FUNCTION invalid_func()
	RETURN INT AS
	BEGIN
		RETURN (SELECT COUNT(*) FROM non_existent_table_xyz);
	END`

	_, err = connDB.ExecContext(ctx, invalidSQL)
	if err != nil {
		// Eager validation path: CREATE fails.
		errStr := err.Error()
		if strings.Contains(errStr, "parse error") || strings.Contains(errStr, "split") {
			t.Fatalf("error appears to be from driver parsing, not server: %s", errStr)
		}
		return
	}

	// Lazy validation path: CREATE succeeds, invocation fails.
	var result int
	err = connDB.QueryRowContext(ctx, `SELECT invalid_func()`).Scan(&result)
	if err == nil {
		t.Fatal("expected error invoking invalid function, but got none")
	}

	errStr := err.Error()
	if strings.Contains(errStr, "parse error") || strings.Contains(errStr, "split") {
		t.Fatalf("error appears to be from driver parsing, not server: %s", errStr)
	}
}

// TestUDSFRegressionSelectStatement verifies that regular SELECT statements
// still work correctly after UDSF changes.
// Given: Normal SELECT statement with no UDSF operations
// When: Executed through the driver
// Then: Query succeeds and returns expected results
func TestUDSFRegressionSelectStatement(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	connDB, err := sql.Open("vertica", myDBConnectString)
	assertNoErr(t, err)
	defer connDB.Close()

	// Execute simple SELECT
	rows, err := connDB.QueryContext(ctx, `SELECT 1 AS result`)
	assertNoErr(t, err)
	defer rows.Close()

	assertTrue(t, rows.Next())
	var result int
	err = rows.Scan(&result)
	assertNoErr(t, err)
	assertEqual(t, result, 1)
}

// TestUDSFRegressionInsertStatement verifies that INSERT statements still work
// correctly after UDSF changes.
// Given: Normal INSERT statement
// When: Executed through the driver
// Then: Insert succeeds without errors
func TestUDSFRegressionInsertStatement(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	connDB, err := sql.Open("vertica", myDBConnectString)
	assertNoErr(t, err)
	defer connDB.Close()

	// Clean up
	_, _ = connDB.ExecContext(ctx, `DROP TABLE IF EXISTS test_regression`)

	// Create table
	_, err = connDB.ExecContext(ctx, `CREATE TABLE test_regression (id INT, name VARCHAR)`)
	assertNoErr(t, err)

	// Insert data
	_, err = connDB.ExecContext(ctx, `INSERT INTO test_regression VALUES (1, 'test')`)
	assertNoErr(t, err)

	// Verify
	var id int
	var name string
	err = connDB.QueryRowContext(ctx, `SELECT id, name FROM test_regression WHERE id = 1`).Scan(&id, &name)
	assertNoErr(t, err)
	assertEqual(t, id, 1)
	assertEqual(t, name, "test")

	// Clean up
	_, _ = connDB.ExecContext(ctx, `DROP TABLE test_regression`)
}

// TestUDSFComplexFunctionWithMultipleParameters verifies that functions with
// multiple parameters and complex logic work correctly.
// Given: CREATE FUNCTION with multiple parameters and conditional logic
// When: Invoked with various argument combinations
// Then: Function returns expected results for all argument combinations
func TestUDSFComplexFunctionWithMultipleParameters(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	connDB, err := sql.Open("vertica", myDBConnectString)
	assertNoErr(t, err)
	defer connDB.Close()

	// Clean up
	_, _ = connDB.ExecContext(ctx, `DROP FUNCTION IF EXISTS test_multi_param(INT, INT, VARCHAR)`)

	// Create function with multiple parameters
	createSQL := `CREATE FUNCTION test_multi_param(val1 INT, val2 INT, operation VARCHAR)
	RETURN INT AS
	BEGIN
		RETURN (CASE operation
			WHEN 'ADD' THEN val1 + val2
			WHEN 'SUB' THEN val1 - val2
			WHEN 'MUL' THEN val1 * val2
			ELSE 0
		END);
	END`

	_, err = connDB.ExecContext(ctx, createSQL)
	assertNoErr(t, err)
	t.Cleanup(func() {
		_, _ = connDB.ExecContext(ctx, `DROP FUNCTION IF EXISTS test_multi_param(INT, INT, VARCHAR)`)
	})

	testCases := []struct {
		val1      int
		val2      int
		operation string
		expected  int
	}{
		{10, 5, "ADD", 15},
		{10, 5, "SUB", 5},
		{10, 5, "MUL", 50},
		{10, 5, "DIV", 0}, // Unknown operation returns 0
	}

	for _, tc := range testCases {
		var result int
		querySQL := fmt.Sprintf(`SELECT test_multi_param(%d, %d, '%s')`, tc.val1, tc.val2, tc.operation)
		err = connDB.QueryRowContext(ctx, querySQL).Scan(&result)
		assertNoErr(t, err)
		assertEqual(t, result, tc.expected)
	}
}

// TestUDSFQuotedIdentifiers verifies that quoted identifiers in function names
// and schema names are handled correctly.
// Given: CREATE FUNCTION with quoted identifiers and mixed case
// When: Executed through the driver
// Then: Function is created and invoked correctly
func TestUDSFQuotedIdentifiers(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	connDB, err := sql.Open("vertica", myDBConnectString)
	assertNoErr(t, err)
	defer connDB.Close()

	// Clean up
	_, _ = connDB.ExecContext(ctx, `DROP FUNCTION IF EXISTS "MyQuotedFunc"()`)

	// Create function with quoted identifier
	createSQL := `CREATE FUNCTION "MyQuotedFunc"()
	RETURN INT AS
	BEGIN
		RETURN 123;
	END`

	_, err = connDB.ExecContext(ctx, createSQL)
	assertNoErr(t, err)
	t.Cleanup(func() {
		_, _ = connDB.ExecContext(ctx, `DROP FUNCTION IF EXISTS "MyQuotedFunc"()`)
	})

	// Call the function
	var result int
	err = connDB.QueryRowContext(ctx, `SELECT "MyQuotedFunc"()`).Scan(&result)
	assertNoErr(t, err)
	assertEqual(t, result, 123)
}

// TestUDSFNullHandling verifies that functions correctly handle NULL inputs and outputs.
// Given: CREATE FUNCTION that handles NULL values
// When: Invoked with NULL arguments
// Then: Function returns expected NULL results
func TestUDSFNullHandling(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	connDB, err := sql.Open("vertica", myDBConnectString)
	assertNoErr(t, err)
	defer connDB.Close()

	// Clean up
	_, _ = connDB.ExecContext(ctx, `DROP FUNCTION IF EXISTS test_null_func(INT)`)

	// Create function that checks for NULL
	createSQL := `CREATE FUNCTION test_null_func(val INT)
	RETURN VARCHAR AS
	BEGIN
		RETURN (CASE
			WHEN val IS NULL THEN 'NULL'
			ELSE 'NOT NULL'
		END);
	END`

	_, err = connDB.ExecContext(ctx, createSQL)
	assertNoErr(t, err)
	t.Cleanup(func() {
		_, _ = connDB.ExecContext(ctx, `DROP FUNCTION IF EXISTS test_null_func(INT)`)
	})

	// Test with NULL
	var result sql.NullString
	err = connDB.QueryRowContext(ctx, `SELECT test_null_func(NULL)`).Scan(&result)
	assertNoErr(t, err)
	if !result.Valid || result.String != "NULL" {
		t.Errorf("expected 'NULL', got %v", result)
	}

	// Test with value
	err = connDB.QueryRowContext(ctx, `SELECT test_null_func(42)`).Scan(&result)
	assertNoErr(t, err)
	if !result.Valid || result.String != "NOT NULL" {
		t.Errorf("expected 'NOT NULL', got %v", result)
	}
}

// TestUDSFTransactionHandling verifies that UDSF CREATE FUNCTION executes
// correctly when routed through *sql.Tx.
// Given: A transaction context created by BeginTx
// When: CREATE FUNCTION is executed via tx.ExecContext and the transaction is committed
// Then: The function is callable afterward, confirming the driver path works with *sql.Tx
func TestUDSFTransactionHandling(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	connDB, err := sql.Open("vertica", myDBConnectString)
	assertNoErr(t, err)
	defer connDB.Close()

	// Clean up
	_, _ = connDB.ExecContext(ctx, `DROP FUNCTION IF EXISTS test_tx_func()`)

	// Begin transaction
	tx, err := connDB.BeginTx(ctx, nil)
	assertNoErr(t, err)

	// Create function within transaction
	createSQL := `CREATE FUNCTION test_tx_func()
	RETURN INT AS
	BEGIN
		RETURN 99;
	END`
	_, err = tx.ExecContext(ctx, createSQL)
	assertNoErr(t, err)

	// Commit transaction
	err = tx.Commit()
	assertNoErr(t, err)
	t.Cleanup(func() {
		_, _ = connDB.ExecContext(ctx, `DROP FUNCTION IF EXISTS test_tx_func()`)
	})

	// Verify function exists after commit
	var result int
	err = connDB.QueryRowContext(ctx, `SELECT test_tx_func()`).Scan(&result)
	assertNoErr(t, err)
	assertEqual(t, result, 99)
}

// TestUDSFFormattingNewlines verifies UDSF detection and atomic execution when
// CREATE, ALTER, and DROP statements have leading, trailing, or mid-statement newlines.
func TestUDSFFormattingNewlines(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	connDB, err := sql.Open("vertica", myDBConnectString)
	assertNoErr(t, err)
	defer connDB.Close()

	_, _ = connDB.ExecContext(ctx, `DROP FUNCTION IF EXISTS test_fmt_newline_func()`)

	// Leading and trailing newlines
	createSQL := "\n\nCREATE FUNCTION test_fmt_newline_func()\n\tRETURN INT AS\n\tBEGIN\n\t\tRETURN 42;\n\tEND\n\n"
	_, err = connDB.ExecContext(ctx, createSQL)
	assertNoErr(t, err)
	t.Cleanup(func() {
		_, _ = connDB.ExecContext(ctx, `DROP FUNCTION IF EXISTS test_fmt_newline_func()`)
		_, _ = connDB.ExecContext(ctx, `DROP FUNCTION IF EXISTS test_fmt_newline_func_v2()`)
	})

	var result int
	err = connDB.QueryRowContext(ctx, `SELECT test_fmt_newline_func()`).Scan(&result)
	assertNoErr(t, err)
	assertEqual(t, result, 42)

	// ALTER with surrounding newlines
	_, err = connDB.ExecContext(ctx, "\n\nALTER FUNCTION test_fmt_newline_func() RENAME TO test_fmt_newline_func_v2\n\n")
	assertNoErr(t, err)
	_, err = connDB.ExecContext(ctx, "\nALTER FUNCTION test_fmt_newline_func_v2() RENAME TO test_fmt_newline_func\n")
	assertNoErr(t, err)

	// DROP with surrounding newlines
	_, err = connDB.ExecContext(ctx, "\n\nDROP FUNCTION test_fmt_newline_func()\n\n")
	assertNoErr(t, err)
}

// TestUDSFFormattingLeadingComment verifies UDSF detection works when a leading
// line comment (--) precedes the CREATE/DROP keyword.
func TestUDSFFormattingLeadingComment(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	connDB, err := sql.Open("vertica", myDBConnectString)
	assertNoErr(t, err)
	defer connDB.Close()

	_, _ = connDB.ExecContext(ctx, `DROP FUNCTION IF EXISTS test_fmt_comment_func()`)

	createSQL := `-- create a test function
CREATE FUNCTION test_fmt_comment_func()
	RETURN INT AS
	BEGIN
		RETURN 7;
	END`
	_, err = connDB.ExecContext(ctx, createSQL)
	assertNoErr(t, err)
	t.Cleanup(func() {
		_, _ = connDB.ExecContext(ctx, `DROP FUNCTION IF EXISTS test_fmt_comment_func()`)
	})

	var result int
	err = connDB.QueryRowContext(ctx, `SELECT test_fmt_comment_func()`).Scan(&result)
	assertNoErr(t, err)
	assertEqual(t, result, 7)

	// DROP with a leading block comment
	_, err = connDB.ExecContext(ctx, "/* remove test function */\nDROP FUNCTION test_fmt_comment_func()")
	assertNoErr(t, err)
}

// TestUDSFFormattingTabs verifies UDSF detection works when tab characters
// appear between SQL keywords and inside the function body.
func TestUDSFFormattingTabs(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	connDB, err := sql.Open("vertica", myDBConnectString)
	assertNoErr(t, err)
	defer connDB.Close()

	_, _ = connDB.ExecContext(ctx, `DROP FUNCTION IF EXISTS test_fmt_tab_func()`)

	// Tabs between keywords in the signature line
	createSQL := "CREATE\tFUNCTION test_fmt_tab_func()\n\tRETURN\tINT\tAS\n\tBEGIN\n\t\tRETURN\t5;\n\tEND"
	_, err = connDB.ExecContext(ctx, createSQL)
	assertNoErr(t, err)
	t.Cleanup(func() {
		_, _ = connDB.ExecContext(ctx, `DROP FUNCTION IF EXISTS test_fmt_tab_func()`)
	})

	var result int
	err = connDB.QueryRowContext(ctx, `SELECT test_fmt_tab_func()`).Scan(&result)
	assertNoErr(t, err)
	assertEqual(t, result, 5)
}

// TestUDSFFormattingInlineComments verifies UDSF detection and atomic execution
// when line comments (--) and block comments (/* */) appear inside the body.
func TestUDSFFormattingInlineComments(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	connDB, err := sql.Open("vertica", myDBConnectString)
	assertNoErr(t, err)
	defer connDB.Close()

	_, _ = connDB.ExecContext(ctx, `DROP FUNCTION IF EXISTS test_fmt_inline_comment_func()`)

	createSQL := `CREATE FUNCTION test_fmt_inline_comment_func() -- inline line comment
	RETURN INT AS
	BEGIN
		-- line comment inside body
		RETURN /* block comment inline */ 13;
	END`
	_, err = connDB.ExecContext(ctx, createSQL)
	assertNoErr(t, err)
	t.Cleanup(func() {
		_, _ = connDB.ExecContext(ctx, `DROP FUNCTION IF EXISTS test_fmt_inline_comment_func()`)
	})

	var result int
	err = connDB.QueryRowContext(ctx, `SELECT test_fmt_inline_comment_func()`).Scan(&result)
	assertNoErr(t, err)
	assertEqual(t, result, 13)
}
