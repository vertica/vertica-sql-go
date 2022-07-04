package vertigo

// Copyright (c) 2019-2022 Micro Focus or one of its affiliates.
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
	"context"
	"crypto/tls"
	"crypto/x509"
	"database/sql"
        "encoding/hex"
	"errors"
	"flag"
	"fmt"
	"io/ioutil"
	"os"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/vertica/vertica-sql-go/logger"
)

var (
	testLogger            = logger.New("test")
	myDBConnectString     string
	otherConnectString    string
	badConnectString      string
	failoverConnectString string
	ctx                   context.Context
)

// The following assert functions compensate for Go having no native assertions
func assertTrue(t *testing.T, v bool) {
	t.Helper()

	if v {
		return
	}

	t.Fatal("value was not true")
}

func assertEqual(t *testing.T, a interface{}, b interface{}) {
	t.Helper()

	if a == b {
		return
	}

	t.Fatal(fmt.Sprintf("%v != %v", a, b))
}

func assertNoErr(t *testing.T, e error) {
	t.Helper()

	if e == nil {
		return
	}

	t.Fatal(e.Error())
}

func assertExecSQL(t *testing.T, connDB *sql.DB, script ...interface{}) {
	t.Helper()

	contents, err := ioutil.ReadFile(fmt.Sprintf("resources/tests/driver_test/%v.sql", script[0]))
	strContents := fmt.Sprintf(string(contents), script[1:]...)

	assertNoErr(t, err)

	for _, cmd := range strings.Split(strContents, ";") {
		trimmedCmd := strings.TrimSpace(cmd)

		if len(trimmedCmd) > 0 {
			testLogger.Debug("sending command: %s", trimmedCmd)
			_, err = connDB.ExecContext(ctx, trimmedCmd)
			assertNoErr(t, err)
		}
	}
}

func assertErr(t *testing.T, err error, errorSubstring string) {
	t.Helper()

	if err == nil {
		t.Fatalf("expected error containing '%s', but there was no error at all", errorSubstring)
	}

	errStr := err.Error()

	if strings.Contains(errStr, errorSubstring) {
		return
	}

	t.Fatalf("expected an error containing '%s', but found '%s'", errorSubstring, errStr)
}

func assertNext(t *testing.T, rows *sql.Rows) {
	t.Helper()

	if !rows.Next() {
		t.Fatal("another row was expected to be available, but wasn't")
	}
}

func assertNoNext(t *testing.T, rows *sql.Rows) {
	t.Helper()

	if rows.Next() {
		t.Fatal("no more rows expected available, but were")
	}
}

func openConnection(t *testing.T, setupScript ...interface{}) *sql.DB {
	connDB, err := sql.Open("vertica", myDBConnectString)
	assertNoErr(t, err)

	err = connDB.PingContext(ctx)
	assertNoErr(t, err)

	if len(setupScript) > 0 {
		assertExecSQL(t, connDB, setupScript...)
	}

	return connDB
}

func closeConnection(t *testing.T, connDB *sql.DB, teardownScript ...interface{}) {
	if len(teardownScript) > 0 {
		assertExecSQL(t, connDB, teardownScript...)
	}
	assertNoErr(t, connDB.Close())
}

func TestTLSConfiguration(t *testing.T) {
	connDB := openConnection(t)
	defer closeConnection(t, connDB)
	rows, err := connDB.QueryContext(ctx, "SELECT ssl_state FROM sessions")
	assertNoErr(t, err)
	defer rows.Close()

	var sslState string
	for rows.Next() {
		assertNoErr(t, rows.Scan(&sslState))
		switch *tlsMode {
		case "none":
			assertEqual(t, sslState, "None")
		case "server", "server-strict":
			assertEqual(t, sslState, "Server")
		case "custom":
			assertEqual(t, sslState, "Mutual")
		default:
			t.Fatalf("tlsmode is set to '%s' but session ssl_state is '%s'", *tlsMode, sslState)
		}
	}
}

func TestBasicQuery(t *testing.T) {
	connDB := openConnection(t)
	defer closeConnection(t, connDB)

	rows, err := connDB.QueryContext(ctx, "SELECT * FROM v_monitor.cpu_usage LIMIT 5")
	assertNoErr(t, err)

	defer rows.Close()

	columnNames, _ := rows.Columns()
	for _, columnName := range columnNames {
		testLogger.Debug("%s", columnName)
	}

	for rows.Next() {
		var nodeName string
		var startTime string
		var endTime string
		var avgCPU float64

		assertNoErr(t, rows.Scan(&nodeName, &startTime, &endTime, &avgCPU))

		testLogger.Debug("%s\t%s\t%s\t%f", nodeName, startTime, endTime, avgCPU)
	}

	rows2, err := connDB.QueryContext(ctx, "SELECT DISTINCT(keyword) FROM v_catalog.standard_keywords WHERE reserved='R' LIMIT 10")
	assertNoErr(t, err)

	defer rows2.Close()

	for rows2.Next() {
		var keyword string
		assertNoErr(t, rows2.Scan(&keyword))

		testLogger.Debug("\"%s\": true,", keyword)
	}

}

func TestBasicNamedArgs(t *testing.T) {
	connDB := openConnection(t)
	defer closeConnection(t, connDB)
	rows, err := connDB.QueryContext(ctx, "SELECT DISTINCT(keyword) FROM v_catalog.standard_keywords WHERE reserved=@type LIMIT 10", sql.Named("type", "R"))
	assertNoErr(t, err)
	defer rows.Close()
	for rows.Next() {
		var keyword string
		assertNoErr(t, rows.Scan(&keyword))
	}
}

func TestPreparedNamedArgs(t *testing.T) {
	connDB := openConnection(t)
	defer closeConnection(t, connDB)
	stmt, err := connDB.PrepareContext(ctx, "SELECT DISTINCT(keyword) FROM v_catalog.standard_keywords WHERE reserved=@type LIMIT 10")
	assertNoErr(t, err)
	rows, err := stmt.QueryContext(ctx, sql.Named("type", "R"))
	assertNoErr(t, err)
	defer rows.Close()
	for rows.Next() {
		var keyword string
		assertNoErr(t, rows.Scan(&keyword))
	}
}

func TestBasicExec(t *testing.T) {
	connDB := openConnection(t, "test_basic_exec_pre")
	defer closeConnection(t, connDB, "test_basic_exec_post")

	res, err := connDB.ExecContext(ctx, "INSERT INTO MyTable VALUES (21, 'Joe Perry')")
	assertNoErr(t, err)

	ct, err := res.RowsAffected()
	assertNoErr(t, err)
	assertEqual(t, ct, int64(1))

	_, err = res.LastInsertId()
	assertNoErr(t, err)
}

func TestBasicArgsQuery(t *testing.T) {
	connDB := openConnection(t, "test_basic_args_query_pre")
	defer closeConnection(t, connDB, "test_basic_args_query_post")

	res, err := connDB.ExecContext(ctx, "INSERT INTO MyTable VALUES (21, 'Joe Perry', true, 123.45, '1950-09-10 13:59:47')")
	assertNoErr(t, err)

	ct, err := res.RowsAffected()
	assertNoErr(t, err)
	assertEqual(t, ct, int64(1))

	//-----------------------------------------------------------------------------------------
	// Make sure we can iterate queries with a string
	//-----------------------------------------------------------------------------------------

	rows, err := connDB.QueryContext(ctx, "SELECT name FROM MyTable WHERE id=?", 21)
	assertNoErr(t, err)
	assertNext(t, rows)

	var nameStr string
	assertNoErr(t, rows.Scan(&nameStr))

	assertEqual(t, nameStr, "Joe Perry")
	assertNoNext(t, rows)

	assertNoErr(t, rows.Close())

	//-----------------------------------------------------------------------------------------
	// Make sure we can run queries with an int, bool and float
	//-----------------------------------------------------------------------------------------

	rows, err = connDB.QueryContext(ctx, "SELECT id, guitarist, height, birthday FROM MyTable WHERE name=?", "Joe Perry")
	assertNoErr(t, err)
	assertNext(t, rows)

	var id int
	var guitarist bool
	var height float64
	var birthday time.Time
	assertNoErr(t, rows.Scan(&id, &guitarist, &height, &birthday))

	assertEqual(t, id, 21)
	assertEqual(t, guitarist, true)
	assertEqual(t, height, 123.45)

	assertEqual(t, birthday.String()[0:19], "1950-09-10 13:59:47") // We gave a timestamp with assumed UTC0, so this is correct.
	assertNoNext(t, rows)

	//-----------------------------------------------------------------------------------------
	// Now see if we can do this are a prepare and a query
	//-----------------------------------------------------------------------------------------

	stmt, err := connDB.PrepareContext(ctx, "SELECT id FROM MyTable WHERE name=?")
	assertNoErr(t, err)

	rows, err = stmt.Query("Joe Perry")
	assertNoErr(t, err)
	assertNext(t, rows)

	assertNoErr(t, rows.Scan(&id))

	assertEqual(t, id, 21)
	assertNoNext(t, rows)

	//-----------------------------------------------------------------------------------------
	// Ensure the 'QueryRowContext()' variant works.
	//-----------------------------------------------------------------------------------------

	err = connDB.QueryRowContext(ctx, "SELECT id FROM MyTable WHERE name=?", "Joe Perry").Scan(&id)
	assertNoErr(t, err)
	assertEqual(t, id, 21)

	assertNoErr(t, rows.Close())
}

func TestBasicArgsWithNil(t *testing.T) {
	connDB := openConnection(t, "test_basic_args_query_pre")
	defer closeConnection(t, connDB, "test_basic_args_query_post")
	var id int
	var name sql.NullString

	//-----------------------------------------------------------------------------------------
	// Ensure we can insert naked null values.
	//-----------------------------------------------------------------------------------------
	_, err := connDB.ExecContext(ctx, "INSERT INTO MyTable VALUES (?, ?, ?, ?, ?)", 13, nil, true, 123.45, time.Now())
	assertNoErr(t, err)

	err = connDB.QueryRowContext(ctx, "SELECT id, name FROM MyTable WHERE name is null").Scan(&id, &name)
	assertNoErr(t, err)
	assertEqual(t, id, 13)
	assertEqual(t, name.Valid, false)

	//-----------------------------------------------------------------------------------------
	// Ensure we can insert null pointers
	//-----------------------------------------------------------------------------------------
	var nullStr *string
	_, err = connDB.ExecContext(ctx, "INSERT INTO MyTable VALUES (?, ?, ?, ?, ?)", 14, nullStr, true, 456.78, time.Now())
	assertNoErr(t, err)

	err = connDB.QueryRowContext(ctx, "SELECT id, name FROM MyTable WHERE id=?", 14).Scan(&id, &name)
	assertNoErr(t, err)
	assertEqual(t, id, 14)
	assertEqual(t, name.Valid, false)

	// -----------------------------------------------------------------------------------------
	// Ensure we can insert NullString with value
	// -----------------------------------------------------------------------------------------
	var someStr = sql.NullString{
		Valid:  true,
		String: "hello",
	}
	_, err = connDB.ExecContext(ctx, "INSERT INTO MyTable VALUES (?, ?, ?, ?, ?)", 15, someStr, true, 456.78, time.Now())
	assertNoErr(t, err)

	err = connDB.QueryRowContext(ctx, "SELECT id, name FROM MyTable WHERE id=?", 15).Scan(&id, &name)
	assertNoErr(t, err)
	assertEqual(t, id, 15)
	assertEqual(t, name.String, "hello")

	// -----------------------------------------------------------------------------------------
	// Ensure we can insert NullString without value
	// -----------------------------------------------------------------------------------------
	var emptyStr = sql.NullString{
		Valid:  false,
		String: "",
	}
	_, err = connDB.ExecContext(ctx, "INSERT INTO MyTable VALUES (?, ?, ?, ?, ?)", 16, emptyStr, true, 456.78, time.Now())
	assertNoErr(t, err)

	err = connDB.QueryRowContext(ctx, "SELECT id, name FROM MyTable WHERE id=?", 16).Scan(&id, &name)
	assertNoErr(t, err)
	assertEqual(t, id, 16)
	assertEqual(t, name.Valid, false)
}

func TestTransaction(t *testing.T) {
	connDB := openConnection(t, "test_transaction_pre")
	defer closeConnection(t, connDB, "test_transaction_post")

	res, err := connDB.ExecContext(ctx, "INSERT INTO MyTable VALUES (21, 'Joe Perry', true, 123.45, '1950-09-10 13:59:47')")
	assertNoErr(t, err)

	ct, err := res.RowsAffected()
	assertNoErr(t, err)
	assertEqual(t, ct, int64(1))

	//-----------------------------------------------------------------------------------------
	// Test Syntaxes for Begin/Commit/Rollback
	//-----------------------------------------------------------------------------------------

	opts := &sql.TxOptions{
		Isolation: sql.LevelDefault,
		ReadOnly:  false,
	}

	tx, err := connDB.BeginTx(ctx, opts)
	assertNoErr(t, err)
	assertNoErr(t, tx.Commit())

	opts.Isolation = sql.LevelReadCommitted
	tx, err = connDB.BeginTx(ctx, opts)
	assertNoErr(t, err)
	assertNoErr(t, tx.Rollback())
}

func TestConnFailover(t *testing.T) {
	// Connection string's "backup_server_node" parameter contains the correct host
	connDB, err := sql.Open("vertica", failoverConnectString)
	assertNoErr(t, err)

	assertNoErr(t, connDB.PingContext(ctx))
	assertNoErr(t, connDB.Close())
}

func TestPWAuthentication(t *testing.T) {

	connDB := openConnection(t, "test_pw_authentication_pre")
	defer closeConnection(t, connDB, "test_pw_authentication_post")

	// Let the user try to login now.
	connDB2, err := sql.Open("vertica", otherConnectString)
	assertNoErr(t, err)
	assertNoErr(t, connDB2.PingContext(ctx))
	assertNoErr(t, connDB2.Close())

	// Try it again with a bad password
	connDB3, err := sql.Open("vertica", badConnectString)
	assertNoErr(t, err)
	err = connDB3.PingContext(ctx)
	if err != nil && err.Error() != "EOF" {
		verr, ok := err.(*VError)
		if !ok {
			t.Fatalf("failed to extract error VError: %v", err)
		}
		assertEqual(t, verr.SQLState, "28000")
		assertEqual(t, verr.Severity, "FATAL")
		assertEqual(t, verr.Routine, "auth_failed")
		assertEqual(t, verr.ErrorCode, "3781")
		assertErr(t, err, "Invalid username or password")
	}
	assertNoErr(t, connDB3.Close())
}

func testAnAuthScheme(t *testing.T, scheme string) {
	connDB := openConnection(t, "test_an_auth_scheme_pre", scheme)
	defer closeConnection(t, connDB, "test_an_auth_scheme_post")

	// Let the user try to login now.
	connDB2, err := sql.Open("vertica", otherConnectString)
	assertNoErr(t, err)

	err = connDB2.PingContext(ctx)
	assertNoErr(t, err)
	assertNoErr(t, connDB2.Close())

	// Try it again with a bad password
	connDB3, err := sql.Open("vertica", badConnectString)
	assertNoErr(t, err)

	err = connDB3.PingContext(ctx)
	if err != nil && err.Error() != "EOF" {
		assertErr(t, err, "Invalid username or password")
	}

	assertNoErr(t, connDB3.Close())
}

func TestMD5Authentication(t *testing.T) {
	testAnAuthScheme(t, "MD5")
}

func TestSHA512Authentication(t *testing.T) {
	testAnAuthScheme(t, "SHA512")
}

func TestDateParsers(t *testing.T) {
	val, err := parseDateColumn("2018-02-01")
	assertNoErr(t, err)
	assertEqual(t, fmt.Sprintf("%s", val)[0:10], "2018-02-01")

	val, err = parseDateColumn("2018-02-01 BC")
	assertNoErr(t, err)
	assertEqual(t, fmt.Sprintf("%s", val)[0:11], "-2018-02-01")
}

func TestTimestampParsers(t *testing.T) {
	val, err := parseTimestampTZColumn("2018-02-01 21:09:33.1234+00")
	assertNoErr(t, err)
	assertEqual(t, fmt.Sprintf("%s", val)[0:30], "2018-02-01 21:09:33.1234 +0000")

	val, err = parseTimestampTZColumn("2018-02-01 21:09:33.123456-06")
	assertNoErr(t, err)
	assertEqual(t, fmt.Sprintf("%s", val)[0:32], "2018-02-01 21:09:33.123456 -0600")

	val, err = parseTimestampTZColumn("2018-01-27 21:09:44+10")
	assertNoErr(t, err)
	assertEqual(t, fmt.Sprintf("%s", val)[0:25], "2018-01-27 21:09:44 +1000")

	val, err = parseTimestampTZColumn("2018-01-27 21:09:44+00")
	assertNoErr(t, err)
	assertEqual(t, fmt.Sprintf("%s", val)[0:25], "2018-01-27 21:09:44 +0000")

	val, err = parseTimestampTZColumn("2018-01-27 21:09:44+10 BC")
	assertNoErr(t, err)
	assertEqual(t, fmt.Sprintf("%s", val)[0:26], "-2018-01-27 21:09:44 +1000")

	val, err = parseTimestampTZColumn("2018-01-27 21:09:44.843913 BC-04")
	assertNoErr(t, err)
	assertEqual(t, fmt.Sprintf("%s", val)[0:33], "-2018-01-27 21:09:44.843913 -0400")
}

func TestEmptyStatementError(t *testing.T) {
	connDB := openConnection(t)
	defer closeConnection(t, connDB)

	// Try as exec.
	_, err := connDB.ExecContext(ctx, "")
	assertErr(t, err, "empty statement")

	// Try as query.
	_, err = connDB.QueryContext(ctx, "")
	assertErr(t, err, "empty statement")
}

func TestValueTypes(t *testing.T) {
	connDB := openConnection(t, "test_value_types_pre")
	defer closeConnection(t, connDB, "test_value_types_post")

	var (
		boolVal          bool
		intVal           int
		floatVal         float64
		charVal          string
		varCharVal       string
		dateVal          time.Time
		timestampVal     time.Time
		timestampTZVal   time.Time
		intervalDayVal   string
		intervalVal      string
		intervalHourVal  string
		intervalHMVal    string
		intervalHSVal    string
		intervalMinVal   string
		intervalMSVal    string
		intervalSecVal   string
		intervalDHVal    string
		intervalDMVal    string
		intervalYearVal  string
		intervalYMVal    string
		intervalMonthVal string
		timeVal          time.Time
		timeTZVal        time.Time
		varBinVal        []byte
		uuidVal          string
		lVarCharVal      string
		lVarBinaryVal    []byte
		binaryVal        []byte
		numericVal       float64
	)

	rows, err := connDB.QueryContext(ctx, "SELECT * FROM full_type_table")
	assertNoErr(t, err)
	assertNext(t, rows)
	assertNoErr(t, rows.Scan(&boolVal, &intVal, &floatVal, &charVal, &varCharVal, &dateVal,
		&timestampVal, &timestampTZVal, &intervalDayVal, &intervalVal, &intervalHourVal,
		&intervalHMVal, &intervalHSVal, &intervalMinVal, &intervalMSVal, &intervalSecVal,
		&intervalDHVal, &intervalDMVal, &intervalYearVal, &intervalYMVal, &intervalMonthVal,
		&timeVal, &timeTZVal, &varBinVal, &uuidVal, &lVarCharVal, &lVarBinaryVal,
		&binaryVal, &numericVal))
	assertEqual(t, boolVal, true)
	assertEqual(t, intVal, 123)
	assertEqual(t, floatVal, 3.141)
	assertEqual(t, charVal, "a")
	assertEqual(t, varCharVal, "test values")
	assertEqual(t, dateVal.String()[0:10], "1999-01-08")
	assertEqual(t, timestampVal.String()[0:26], "2019-08-04 00:45:19.843913")
	assertEqual(t, timestampTZVal.UTC().String()[0:32], "2019-08-04 04:45:19.843913 +0000")
	assertEqual(t, intervalDayVal, "365")
	assertEqual(t, intervalVal, "-6537150 01:03:06.0051")
	assertEqual(t, intervalHourVal, "74")
	assertEqual(t, intervalHMVal, "01:03")
	assertEqual(t, intervalHSVal, "8760:15:20")
	assertEqual(t, intervalMinVal, "15")
	assertEqual(t, intervalMSVal, "525605:20")
	assertEqual(t, intervalSecVal, "216901.24")
	assertEqual(t, intervalDHVal, "-2 12")
	assertEqual(t, intervalDMVal, "-2 12:15")
	assertEqual(t, intervalYearVal, "1")
	assertEqual(t, intervalYMVal, "1-2")
	assertEqual(t, intervalMonthVal, "22")
	assertEqual(t, timeVal.String()[11:23], "04:05:06.789")
	assertEqual(t, timeTZVal.String()[11:25], "16:05:06 -0800")
	assertEqual(t, hex.EncodeToString(varBinVal), "beefdead")
	assertEqual(t, uuidVal, "372fd680-6a72-4003-96b0-10bbe78cd635")
	assertEqual(t, lVarCharVal, "longer var char")
	assertEqual(t, hex.EncodeToString(lVarBinaryVal), "deadbeef")
	assertEqual(t, hex.EncodeToString(binaryVal), "baadf00d")
	assertEqual(t, numericVal, 1.2345)

	assertNext(t, rows)

	var (
		nullBoolVal          sql.NullBool
		nullIntVal           sql.NullInt64
		nullFloatVal         sql.NullFloat64
		nullCharVal          sql.NullString
		nullVarCharVal       sql.NullString
		nullDateVal          sql.NullTime
		nullTimestampVal     sql.NullTime
		nullTimestampTZVal   sql.NullTime
		nullIntervalDayVal   sql.NullString
		nullIntervalVal      sql.NullString
		nullIntervalHourVal  sql.NullString
		nullIntervalHMVal    sql.NullString
		nullIntervalHSVal    sql.NullString
		nullIntervalMinVal   sql.NullString
		nullIntervalMSVal    sql.NullString
		nullIntervalSecVal   sql.NullString
		nullIntervalDHVal    sql.NullString
		nullIntervalDMVal    sql.NullString
		nullIntervalYearVal  sql.NullString
		nullIntervalYMVal    sql.NullString
		nullIntervalMonthVal sql.NullString
		nullTimeVal          sql.NullTime
		nullTimeTZVal        sql.NullTime
		nullVarBinVal        sql.NullString
		nullUuidVal          sql.NullString
		nullLVarCharVal      sql.NullString
		nullLVarBinaryVal    sql.NullString
		nullBinaryVal        sql.NullString
		nullNumericVal       sql.NullFloat64
	)

	assertNoErr(t, rows.Scan(&nullBoolVal, &nullIntVal, &nullFloatVal, &nullCharVal,
		&nullVarCharVal, &nullDateVal, &nullTimestampVal, &nullTimestampTZVal,
		&nullIntervalDayVal, &nullIntervalVal, &nullIntervalHourVal, &nullIntervalHMVal,
		&nullIntervalHSVal, &nullIntervalMinVal, &nullIntervalMSVal,
		&nullIntervalSecVal, &nullIntervalDHVal, &nullIntervalDMVal,
		&nullIntervalYearVal, &nullIntervalYMVal, &nullIntervalMonthVal,
		&nullTimeVal, &nullTimeTZVal, &nullVarBinVal, &nullUuidVal,
		&nullLVarCharVal, &nullLVarBinaryVal, &nullBinaryVal, &nullNumericVal))

	assertTrue(t, !nullBoolVal.Valid)
	assertTrue(t, !nullIntVal.Valid)
	assertTrue(t, !nullFloatVal.Valid)
	assertTrue(t, !nullCharVal.Valid)
	assertTrue(t, !nullVarCharVal.Valid)
	assertTrue(t, !nullDateVal.Valid)
	assertTrue(t, !nullTimestampVal.Valid)
	assertTrue(t, !nullTimestampTZVal.Valid)
	assertTrue(t, !nullIntervalDayVal.Valid)
	assertTrue(t, !nullIntervalVal.Valid)
	assertTrue(t, !nullIntervalHourVal.Valid)
	assertTrue(t, !nullIntervalHMVal.Valid)
	assertTrue(t, !nullIntervalHSVal.Valid)
	assertTrue(t, !nullIntervalMinVal.Valid)
	assertTrue(t, !nullIntervalMSVal.Valid)
	assertTrue(t, !nullIntervalSecVal.Valid)
	assertTrue(t, !nullIntervalDHVal.Valid)
	assertTrue(t, !nullIntervalDMVal.Valid)
	assertTrue(t, !nullIntervalYearVal.Valid)
	assertTrue(t, !nullIntervalYMVal.Valid)
	assertTrue(t, !nullIntervalMonthVal.Valid)
	assertTrue(t, !nullTimeVal.Valid)
	assertTrue(t, !nullTimeTZVal.Valid)
	assertTrue(t, !nullVarBinVal.Valid)
	assertTrue(t, !nullUuidVal.Valid)
	assertTrue(t, !nullLVarCharVal.Valid)
	assertTrue(t, !nullLVarBinaryVal.Valid)
	assertTrue(t, !nullBinaryVal.Valid)
	assertTrue(t, !nullNumericVal.Valid)

	var columns = []struct {
		name             string
		databaseTypeName string
		scanType         reflect.Type
		nullable         bool
		length           int64
		lengthOk         bool
		precision        int64
		scale            int64
		decimalSizeOk    bool
	}{
		{"boolVal", "BOOL", reflect.TypeOf(sql.NullBool{}), true, 1, false, 0, 0, false},
		{"intVal", "INT", reflect.TypeOf(sql.NullInt64{}), true, 8, false, 0, 0, false},
		{"floatVal", "FLOAT", reflect.TypeOf(sql.NullFloat64{}), true, 8, false, 0, 0, false},
		{"charVal", "CHAR", reflect.TypeOf(sql.NullString{}), true, 1, true, 0, 0, false},
		{"varCharVal", "VARCHAR", reflect.TypeOf(sql.NullString{}), true, 128, true, 0, 0, false},
		{"dateVal", "DATE", reflect.TypeOf(sql.NullTime{}), true, 8, false, 0, 0, false},
		{"timestampVal", "TIMESTAMP", reflect.TypeOf(sql.NullTime{}), true, 8, false, 6, 0, true},
		{"timestampTZVal", "TIMESTAMPTZ", reflect.TypeOf(sql.NullTime{}), true, 8, false, 6, 0, true},
		{"intervalDayVal", "INTERVAL DAY", reflect.TypeOf(sql.NullString{}), true, 8, false, 0, 0, true},
		{"intervalVal", "INTERVAL DAY TO SECOND", reflect.TypeOf(sql.NullString{}), true, 8, false, 4, 0, true},
		{"intervalHourVal", "INTERVAL HOUR", reflect.TypeOf(sql.NullString{}), true, 8, false, 0, 0, true},
		{"intervalHMVal", "INTERVAL HOUR TO MINUTE", reflect.TypeOf(sql.NullString{}), true, 8, false, 0, 0, true},
		{"intervalHSVal", "INTERVAL HOUR TO SECOND", reflect.TypeOf(sql.NullString{}), true, 8, false, 6, 0, true},
		{"intervalMinVal", "INTERVAL MINUTE", reflect.TypeOf(sql.NullString{}), true, 8, false, 0, 0, true},
		{"intervalMSVal", "INTERVAL MINUTE TO SECOND", reflect.TypeOf(sql.NullString{}), true, 8, false, 6, 0, true},
		{"intervalSecVal", "INTERVAL SECOND", reflect.TypeOf(sql.NullString{}), true, 8, false, 2, 0, true},
		{"intervalDHVal", "INTERVAL DAY TO HOUR", reflect.TypeOf(sql.NullString{}), true, 8, false, 0, 0, true},
		{"intervalDMVal", "INTERVAL DAY TO MINUTE", reflect.TypeOf(sql.NullString{}), true, 8, false, 0, 0, true},
		{"intervalYearVal", "INTERVAL YEAR", reflect.TypeOf(sql.NullString{}), true, 8, false, 0, 0, true},
		{"intervalYMVal", "INTERVAL YEAR TO MONTH", reflect.TypeOf(sql.NullString{}), true, 8, false, 0, 0, true},
		{"intervalMonthVal", "INTERVAL MONTH", reflect.TypeOf(sql.NullString{}), true, 8, false, 0, 0, true},
		{"timeVal", "TIME", reflect.TypeOf(sql.NullTime{}), true, 8, false, 6, 0, true},
		{"timeTZVal", "TIMETZ", reflect.TypeOf(sql.NullTime{}), true, 8, false, 6, 0, true},
		{"varBinVal", "VARBINARY", reflect.TypeOf(sql.NullString{}), true, 80, true, 0, 0, false},
		{"uuidVal", "UUID", reflect.TypeOf(sql.NullString{}), true, 16, false, 0, 0, false},
		{"lVarCharVal", "LONG VARCHAR", reflect.TypeOf(sql.NullString{}), true, 65536, true, 0, 0, false},
		{"lVarBinaryVal", "LONG VARBINARY", reflect.TypeOf(sql.NullString{}), true, 65536, true, 0, 0, false},
		{"binaryVal", "BINARY", reflect.TypeOf(sql.NullString{}), true, 1, true, 0, 0, false},
		{"numericVal", "NUMERIC", reflect.TypeOf(sql.NullFloat64{}), true, 24, true, 40, 18, true},
	}

	// Read column types
	colTypes, err := rows.ColumnTypes()
	assertNoErr(t, err)
	for i, column := range colTypes {
		col := columns[i]

		// Name
		name := column.Name()
		assertEqual(t, name, col.name)

		// DatabaseTypeName
		databaseTypeName := column.DatabaseTypeName()
		assertEqual(t, databaseTypeName, col.databaseTypeName)

		// ScanType
		scanType := column.ScanType()
		assertEqual(t, scanType, col.scanType)

		// Nullable
		nullable, ok := column.Nullable()
		assertTrue(t, ok)
		assertEqual(t, nullable, col.nullable)

		// Length
		length, isVariable := column.Length()
		assertEqual(t, length, col.length)
		assertEqual(t, isVariable, col.lengthOk)

		// Precision and scale
		precision, scale, decimalSizeOk := column.DecimalSize()
		assertEqual(t, precision, col.precision)
		assertEqual(t, scale, col.scale)
		assertEqual(t, decimalSizeOk, col.decimalSizeOk)
	}

	assertNoErr(t, rows.Close())
}

// Issue 17 : Reusing prepared statements throws runtime errors
// https://github.com/vertica/vertica-sql-go/issues/17
func TestStmtReuseBug(t *testing.T) {
	connDB := openConnection(t)
	defer closeConnection(t, connDB)

	var res bool

	stmt, err := connDB.PrepareContext(ctx, "SELECT true AS res")
	assertNoErr(t, err)

	// first call
	rows, err := stmt.QueryContext(ctx)
	assertNoErr(t, err)

	defer rows.Close()

	assertNext(t, rows)
	assertNoErr(t, rows.Scan(&res))
	assertEqual(t, res, true)
	assertNoNext(t, rows)

	// second call
	rows, err = stmt.QueryContext(ctx)
	assertNoErr(t, err)

	defer rows.Close()

	assertNext(t, rows)
	assertNoErr(t, rows.Scan(&res))
	assertEqual(t, res, true)
	assertNoNext(t, rows)
}

// Issue 20 : No columns returned when query returns no rows
// https://github.com/vertica/vertica-sql-go/issues/20
func TestColumnsWithNoRows(t *testing.T) {
	connDB := openConnection(t)
	defer closeConnection(t, connDB)

	stmt, err := connDB.PrepareContext(ctx, "SELECT true AS res WHERE false")
	assertNoErr(t, err)

	defer stmt.Close()

	rows, err := stmt.QueryContext(ctx)
	assertNoErr(t, err)

	columns, err := rows.Columns()
	assertNoErr(t, err)

	defer rows.Close()

	assertEqual(t, len(columns), 1)

	assertNoNext(t, rows)
}

type threadedQuery struct {
	query         string
	resultColumns []string
}

// Issue 22 : Possible issue with wrong rows returned from current stmt results
// https://github.com/vertica/vertica-sql-go/issues/22
func TestStmtOrderingInThreads(t *testing.T) {
	connDB := openConnection(t, "test_stmt_ordering_threads_pre")
	connDB.SetMaxOpenConns(1)
	defer connDB.SetMaxOpenConns(0)
	defer closeConnection(t, connDB, "test_stmt_ordering_threads_post")

	connDB.SetMaxOpenConns(1)

	queries := []threadedQuery{
		{query: "SELECT a FROM stmt_thread_test", resultColumns: []string{"a"}},
		{query: "SELECT a, b FROM stmt_thread_test", resultColumns: []string{"a", "b"}},
		{query: "SELECT a, b, c FROM stmt_thread_test", resultColumns: []string{"a", "b", "c"}},
		{query: "SELECT a, b, c, d FROM stmt_thread_test", resultColumns: []string{"a", "b", "c", "d"}},
		{query: "SELECT a, b, c, d, e FROM stmt_thread_test", resultColumns: []string{"a", "b", "c", "d", "e"}},
		{query: "SELECT a FROM stmt_thread_test", resultColumns: []string{"a"}},
		{query: "SELECT a, b FROM stmt_thread_test", resultColumns: []string{"a", "b"}},
		{query: "SELECT a, b, c FROM stmt_thread_test", resultColumns: []string{"a", "b", "c"}},
		{query: "SELECT a, b, c, d FROM stmt_thread_test", resultColumns: []string{"a", "b", "c", "d"}},
		{query: "SELECT a, b, c, d, e FROM stmt_thread_test", resultColumns: []string{"a", "b", "c", "d", "e"}},
		{query: "SELECT a FROM stmt_thread_test", resultColumns: []string{"a"}},
		{query: "SELECT a, b FROM stmt_thread_test", resultColumns: []string{"a", "b"}},
		{query: "SELECT a, b, c FROM stmt_thread_test", resultColumns: []string{"a", "b", "c"}},
		{query: "SELECT a, b, c, d FROM stmt_thread_test", resultColumns: []string{"a", "b", "c", "d"}},
		{query: "SELECT a, b, c, d, e FROM stmt_thread_test", resultColumns: []string{"a", "b", "c", "d", "e"}},
		{query: "SELECT a FROM stmt_thread_test", resultColumns: []string{"a"}},
		{query: "SELECT a, b FROM stmt_thread_test", resultColumns: []string{"a", "b"}},
		{query: "SELECT a, b, c FROM stmt_thread_test", resultColumns: []string{"a", "b", "c"}},
		{query: "SELECT a, b, c, d FROM stmt_thread_test", resultColumns: []string{"a", "b", "c", "d"}},
		{query: "SELECT a, b, c, d, e FROM stmt_thread_test", resultColumns: []string{"a", "b", "c", "d", "e"}},
		{query: "SELECT a FROM stmt_thread_test", resultColumns: []string{"a"}},
		{query: "SELECT a, b FROM stmt_thread_test", resultColumns: []string{"a", "b"}},
		{query: "SELECT a, b, c FROM stmt_thread_test", resultColumns: []string{"a", "b", "c"}},
		{query: "SELECT a, b, c, d FROM stmt_thread_test", resultColumns: []string{"a", "b", "c", "d"}},
		{query: "SELECT a, b, c, d, e FROM stmt_thread_test", resultColumns: []string{"a", "b", "c", "d", "e"}},
	}

	var wg sync.WaitGroup
	wg.Add(len(queries))

	for ct, query := range queries {
		go func(idx int, q threadedQuery) {
			defer wg.Done()
			stmt, err := connDB.PrepareContext(ctx, q.query)
			assertNoErr(t, err)
			defer stmt.Close()
			rows, err := stmt.QueryContext(ctx)
			assertNoErr(t, err)
			defer rows.Close()
			assertNext(t, rows)

			columns, err := rows.Columns()
			assertNoErr(t, err)
			assertEqual(t, len(columns), len(q.resultColumns))
		}(ct, query)
	}

	wg.Wait()

}

// Issue 9 : Does it support COPY FROM / COPY TO ?
// https://github.com/vertica/vertica-sql-go/issues/9
func TestSTDINCopy(t *testing.T) {
	connDB := openConnection(t, "test_stdin_copy_pre")
	defer closeConnection(t, connDB, "test_stdin_copy_post")

	// Do some trickery with os.Stdin, but make sure to put it back when we're done.
	oldStdIn := os.Stdin

	fp, err := os.OpenFile("./resources/csv/sample_data.csv", os.O_RDONLY, 0600)
	assertNoErr(t, err)

	os.Stdin = fp
	defer func() { os.Stdin.Close(); os.Stdin = oldStdIn }()

	_, err = connDB.ExecContext(ctx, "COPY stdin_data FROM STDIN DELIMITER ','")
	assertNoErr(t, err)

	rows, err := connDB.QueryContext(ctx, "SELECT name,id FROM stdin_data as t(name,id) order by name")
	assertNoErr(t, err)

	defer rows.Close()

	columns, _ := rows.Columns()
	assertEqual(t, len(columns), 2)

	names := []string{"john", "roger", "siting", "tom", "yang"}
	ids := []int{555, 123, 456, 789, 333}
	matched := 0
	var name string
	var id int

	for rows.Next() {
		assertNoErr(t, rows.Scan(&name, &id))
		assertEqual(t, name, names[matched])
		assertEqual(t, id, ids[matched])
		matched++
	}

	assertEqual(t, matched, 5)
	assertNoNext(t, rows)
}

// Issue 9 : Does it support COPY FROM / COPY TO ?
// https://github.com/vertica/vertica-sql-go/issues/9
func TestSTDINCopyWithStream(t *testing.T) {
	connDB := openConnection(t, "test_stdin_copy_pre")
	defer closeConnection(t, connDB, "test_stdin_copy_post")

	fp, err := os.OpenFile("./resources/csv/sample_data.csv", os.O_RDONLY, 0600)
	assertNoErr(t, err)

	defer fp.Close()

	vCtx := NewVerticaContext(ctx)
	_ = vCtx.SetCopyInputStream(fp)
	_ = vCtx.SetCopyBlockSizeBytes(32768)

	_, err = connDB.ExecContext(vCtx, "COPY stdin_data FROM STDIN DELIMITER ','")
	assertNoErr(t, err)

	rows, err := connDB.QueryContext(ctx, "SELECT name,id FROM stdin_data AS t(name,id) ORDER BY name")
	assertNoErr(t, err)

	defer rows.Close()

	columns, _ := rows.Columns()
	assertEqual(t, len(columns), 2)

	names := []string{"john", "roger", "siting", "tom", "yang"}
	ids := []int{555, 123, 456, 789, 333}
	matched := 0
	var name string
	var id int

	for rows.Next() {
		assertNoErr(t, rows.Scan(&name, &id))
		assertEqual(t, name, names[matched])
		assertEqual(t, id, ids[matched])
		matched++
	}

	assertEqual(t, matched, 5)
	assertNoNext(t, rows)
}

// Issue 44 : error during parsing of prepared statement causes perpetual error state
// https://github.com/vertica/vertica-sql-go/issues/44
func TestHangAfterError(t *testing.T) {
	connDB := openConnection(t)
	defer closeConnection(t, connDB)

	rows, err := connDB.QueryContext(ctx, "SELECT 1")
	defer rows.Close()

	assertNoErr(t, err)
	assertNext(t, rows)
	assertNoNext(t, rows)

	rows, err = connDB.QueryContext(ctx, "SELECT 1+'abcd'")
	verr, ok := err.(*VError)
	if !ok {
		t.Fatalf("failed to extract error VError: %v", err)
	}
	assertEqual(t, verr.SQLState, "22V02")
	assertEqual(t, verr.Severity, "ERROR")
	assertEqual(t, verr.Routine, "scanint8")
	assertEqual(t, verr.ErrorCode, "3681")
	assertErr(t, err, "Invalid input syntax for integer")

	rows, err = connDB.QueryContext(ctx, "SELECT 2")
	defer rows.Close()

	assertNoErr(t, err)
	assertNext(t, rows)
	assertNoNext(t, rows)
}

func testEnableResultCachePageSized(t *testing.T, connDB *sql.DB, ctx VerticaContext, pageSize int) {
	assertNoErr(t, ctx.SetInMemoryResultRowLimit(pageSize))

	rows, _ := connDB.QueryContext(ctx, "SELECT a, b, c, d, e FROM result_cache_test ORDER BY a")
	defer rows.Close()

	var a int
	var b string
	var c bool
	var d float64
	var e int
	var count int

	for rows.Next() {
		count++
		assertNoErr(t, rows.Scan(&a, &b, &c, &d, &e))
		assertEqual(t, a, count)
		assertEqual(t, b, "dog")
		assertEqual(t, c, true)
		assertEqual(t, d, 3.14159)
		assertEqual(t, e, 456)
	}

	assertNoNext(t, rows)
	assertEqual(t, count, 42)
}

// Issue 43 : response batching / cursor / lazy queries
// https://github.com/vertica/vertica-sql-go/issues/43
func TestEnableResultCache(t *testing.T) {
	connDB := openConnection(t, "test_enable_result_cache_pre")
	defer closeConnection(t, connDB, "test_enable_result_cache_post")

	vCtx := NewVerticaContext(context.Background())

	testEnableResultCachePageSized(t, connDB, vCtx, 1)
	testEnableResultCachePageSized(t, connDB, vCtx, 5)
	testEnableResultCachePageSized(t, connDB, vCtx, 49)
	testEnableResultCachePageSized(t, connDB, vCtx, 0)
}

//func TestConnectionClosure(t *testing.T) {
// 	adminDB := openConnection(t, "test_connection_closed_pre")
// 	defer closeConnection(t, adminDB, "test_connection_closed_post")
// 	const userQuery = "select 1 as test"
//
// 	userDB, _ := sql.Open("vertica", otherConnectString)
// 	defer userDB.Close()
// 	rows, err := userDB.Query(userQuery)
// 	assertNoErr(t, err)
// 	rows.Close()
// 	adminDB.Query("select close_user_sessions('TestGuy')")
// 	rows, err = userDB.Query(userQuery)
// 	// Depending on Go version this second query may or may not error
// 	if err == nil {
// 		rows.Close()
// 	}
// 	rows, err = userDB.Query(userQuery)
// 	assertNoErr(t, err) // Should definitely have a working connection again here
// 	rows.Close()
// }

func TestConcurrentStatementQuery(t *testing.T) {
	connDB := openConnection(t, "test_stmt_ordering_threads_pre")
	defer closeConnection(t, connDB, "test_stmt_ordering_threads_post")
	stmt, err := connDB.PrepareContext(ctx, "SELECT a FROM stmt_thread_test")
	assertNoErr(t, err)
	wg := new(sync.WaitGroup)
	wg.Add(3)
	for i := 0; i < 3; i++ {
		go func() {
			defer wg.Done()
			_, err := stmt.QueryContext(ctx)
			assertNoErr(t, err)
		}()
	}
	wg.Wait()
}

func TestInvalidDDLStatement(t *testing.T) {
	connDB := openConnection(t)
	defer closeConnection(t, connDB)
	_, err := connDB.Exec("DROP VIEW DOESNOTEXISTVIEW")
	verr, ok := err.(*VError)
	if !ok {
		t.Fatalf("failed to extract error VError: %v", err)
	}
	assertEqual(t, verr.SQLState, "42704")
	assertEqual(t, verr.Severity, "ROLLBACK")
	assertEqual(t, verr.Routine, "ProcessDrop")
	assertEqual(t, verr.ErrorCode, "5446")
	assertErr(t, err, "does not exist")
}

func TestLockOnError(t *testing.T) {
	connDB := openConnection(t)
	defer closeConnection(t, connDB)

	_, err := connDB.Query("select throw_error('whatever')")
	verr, ok := err.(*VError)
	if !ok {
		t.Fatalf("failed to extract error VError: %v", err)
	}
	assertEqual(t, verr.SQLState, "22V23")
	assertEqual(t, verr.Severity, "ERROR")
	assertEqual(t, verr.Routine, "ThrowUserError")
	assertEqual(t, verr.ErrorCode, "7137")
	assertErr(t, err, "ERROR: whatever")

	_, err = connDB.QueryContext(ctx, "select 1")
	assertNoErr(t, err)
}

func TestUnexpectedResult(t *testing.T) {
	connDB := openConnection(t)
	defer closeConnection(t, connDB)

	_, err := connDB.Query("select throw_error('whatever')")
	assertErr(t, err, "ERROR: whatever")

	_, err = connDB.Query("select throw_error('whatever')")
	assertErr(t, err, "ERROR: whatever")
}

var verticaUserName = flag.String("user", "dbadmin", "the user name to connect to Vertica")
var verticaPassword = flag.String("password", os.Getenv("VERTICA_TEST_PASSWORD"), "Vertica password for this user")
var verticaHostPort = flag.String("locator", "localhost:5433", "Vertica's host and port")
var tlsMode = flag.String("tlsmode", "none", "SSL/TLS mode (none, server, server-strict, custom)")
var usePreparedStmts = flag.Bool("use_prepared_statements", true, "whether to use prepared statements for all queries/executes")

const (
	keyPath    string = "resources/tests/ssl/client.key"
	crtPath    string = "resources/tests/ssl/client.crt"
	caCertPath string = "resources/tests/ssl/rootCA.crt"
)

func getCerts(crtPath, keyPath string) ([]tls.Certificate, error) {
	if _, err := os.Stat(crtPath); err != nil {
		return nil, err
	}
	if _, err := os.Stat(keyPath); err != nil {
		return nil, err
	}
	cert, err := tls.LoadX509KeyPair(crtPath, keyPath)
	if err != nil {
		return nil, err
	}

	return []tls.Certificate{cert}, nil
}
func getTlsConfig() (*tls.Config, error) {

	certs, err := getCerts(crtPath, keyPath)
	if err != nil {
		return nil, fmt.Errorf("could not get certs: %v", err)
	}
	caCert, err := ioutil.ReadFile(caCertPath)
	if err != nil {
		return nil, fmt.Errorf("could not read cacertfile: %v", err)
	}
	caCertPool := x509.NewCertPool()
	if !caCertPool.AppendCertsFromPEM(caCert) {
		return nil, errors.New("could not append certs from cacert")
	}

	tlsConfig := &tls.Config{
		RootCAs:      caCertPool,
		Certificates: certs,
		ServerName:   "localhost",
	}
	tlsConfig.BuildNameToCertificate()
	return tlsConfig, nil
}

func init() {
	// One or both lines below are necessary depending on your go version
	testing.Init()
	flag.Parse()

	// For debugging.
	// logger.SetLogLevel(logger.INFO)

	testLogger.Info("user name: %s", *verticaUserName)
	testLogger.Info("password : **********")
	testLogger.Info("locator  : %s", *verticaHostPort)
	testLogger.Info("tlsmode  : %s", *tlsMode)

	usePreparedStmtsString := "use_prepared_statements="

	if *usePreparedStmts {
		usePreparedStmtsString += "1"
	} else {
		usePreparedStmtsString += "0"
	}

	if *tlsMode == "custom" {

		testLogger.Info("loading tls config")
		tlsConfig, err := getTlsConfig()
		if err != nil {
			testLogger.Fatal("could not get tls-config: %v", err)
		}
		if err := RegisterTLSConfig("custom", tlsConfig); err != nil {
			testLogger.Fatal("could not register tls config: %v", err)
		}
	}
	myDBConnectString = "vertica://" + *verticaUserName + ":" + *verticaPassword + "@" + *verticaHostPort + "/" + *verticaUserName + "?" + usePreparedStmtsString + "&tlsmode=" + *tlsMode
	otherConnectString = "vertica://TestGuy:TestGuyPass@" + *verticaHostPort + "/TestGuy?tlsmode=" + *tlsMode
	badConnectString = "vertica://TestGuy:TestGuyBadPass@" + *verticaHostPort + "/TestGuy?tlsmode=" + *tlsMode
	failoverConnectString = "vertica://" + *verticaUserName + ":" + *verticaPassword + "@badHost" + "/" + *verticaUserName + "?backup_server_node=abc.com:100000," + *verticaHostPort + ",localhost:port"

	ctx = context.Background()
}
