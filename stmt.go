package vertigo

// Copyright (c) 2019 Micro Focus or one of its affiliates.
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
	"database/sql/driver"
	"fmt"
	"io"
	"math/rand"
	"os"
	"reflect"
	"strings"
	"time"

	"github.com/vertica/vertica-sql-go/common"
	"github.com/vertica/vertica-sql-go/logger"
	"github.com/vertica/vertica-sql-go/msgs"
)

var (
	stmtLogger = logger.New("stmt")
)

type parseState int

const (
	parseStateUnparsed parseState = iota
	parseStateParseError
	parseStateParsed
)

type stmt struct {
	conn         *connection
	command      string
	preparedName string
	parseState   parseState
	paramTypes   []common.ParameterType
	lastRowDesc  *msgs.BERowDescMsg
}

func newStmt(connection *connection, command string) (*stmt, error) {

	if len(command) == 0 {
		return nil, fmt.Errorf("cannot create an empty statement")
	}

	return &stmt{
		conn:         connection,
		command:      command,
		preparedName: fmt.Sprintf("S%d%d%d", os.Getpid(), time.Now().Unix(), rand.Int31()),
		parseState:   parseStateUnparsed,
	}, nil
}

// Close closes this statement.
func (s *stmt) Close() error {
	if s.parseState == parseStateParsed {
		closeMsg := &msgs.FECloseMsg{TargetType: msgs.CmdTargetTypeStatement, TargetName: s.preparedName}
		if err := s.conn.sendMessage(closeMsg); err != nil {
			return err
		}
		s.parseState = parseStateUnparsed
	}

	return nil
}

// NumInput docs
func (s *stmt) NumInput() int {
	return strings.Count(s.command, "?")
}

// Exec docs
func (s *stmt) Exec(args []driver.Value) (driver.Result, error) {

	namedArgs := make([]driver.NamedValue, len(args))
	for idx, arg := range args {
		namedArgs[idx] = driver.NamedValue{
			Name:    "",
			Ordinal: idx,
			Value:   arg,
		}
	}
	return s.ExecContext(context.Background(), namedArgs)
}

// Query docs
func (s *stmt) Query(args []driver.Value) (driver.Rows, error) {
	stmtLogger.Debug("stmt.Query(): %s\n", s.command)

	namedArgs := make([]driver.NamedValue, len(args))
	for idx, arg := range args {
		namedArgs[idx] = driver.NamedValue{
			Name:    "",
			Ordinal: idx,
			Value:   arg,
		}
	}
	return s.QueryContext(context.Background(), namedArgs)
}

// ExecContext docs
func (s *stmt) ExecContext(ctx context.Context, args []driver.NamedValue) (driver.Result, error) {
	stmtLogger.Trace("stmt.ExecContext()")

	rows, err := s.QueryContext(ctx, args)

	if err != nil {
		return driver.ResultNoRows, err
	}

	numCols := len(rows.Columns())
	vals := make([]driver.Value, numCols)

	if rows.Next(vals) == io.EOF {
		return driver.ResultNoRows, nil
	}

	rv := reflect.ValueOf(vals[0])

	return &result{lastInsertID: 0, rowsAffected: rv.Int()}, nil
}

func (s *stmt) QueryContext(ctx context.Context, args []driver.NamedValue) (driver.Rows, error) {
	return s.QueryContextRaw(ctx, args)
}

// QueryContext docs
func (s *stmt) QueryContextRaw(ctx context.Context, args []driver.NamedValue) (*rows, error) {
	stmtLogger.Debug("stmt.QueryContextRaw(): %s", s.command)

	var cmd string
	var err error
	var portalName string

	// If we have a prepared statement, go through bind/execute() phases instead.
	if s.parseState == parseStateParsed {
		if err = s.bindAndExecute(portalName, args); err != nil {
			return emptyRowSet, err
		}

		return s.collectResults()
	}

	rows := emptyRowSet

	// We aren't a prepared statement, manually interpolate and do as a simple query.
	cmd, err = s.interpolate(args)

	if err != nil {
		return emptyRowSet, err
	}

	if err = s.conn.sendMessage(&msgs.FEQueryMsg{Query: cmd}); err != nil {
		return emptyRowSet, err
	}

	for {
		bMsg, err := s.conn.recvMessage()

		if err != nil {
			return emptyRowSet, err
		}

		switch msg := bMsg.(type) {
		case *msgs.BEDataRowMsg:
			rows.addRow(msg)
		case *msgs.BERowDescMsg:
			rows = newRows(msg, s.conn.serverTZOffset)
		case *msgs.BECmdCompleteMsg:
			break
		case *msgs.BEErrorMsg:
			return emptyRowSet, msg.ToErrorType()
		case *msgs.BEEmptyQueryResponseMsg:
			return emptyRowSet, nil
		case *msgs.BEReadyForQueryMsg, *msgs.BEPortalSuspendedMsg:
			return rows, nil
		default:
			s.conn.defaultMessageHandler(bMsg)
		}
	}
}

func (s *stmt) interpolate(args []driver.NamedValue) (string, error) {

	numArgs := s.NumInput()

	if numArgs == 0 {
		return s.command, nil
	}

	result := s.command

	var replaceStr string

	for _, arg := range args {

		switch v := arg.Value.(type) {
		case int64, float64:
			replaceStr = fmt.Sprintf("%v", v)
		case string:
			replaceStr = fmt.Sprintf("'%s'", v)
		case bool:
			if v {
				replaceStr = "true"
			} else {
				replaceStr = "false"
			}
		case time.Time:
			replaceStr = fmt.Sprintf("%02d-%02d-%02d %02d:%02d:%02d",
				v.Year(),
				v.Month(),
				v.Day(),
				v.Hour(),
				v.Minute(),
				v.Second())
		default:
			replaceStr = "?unknown_type?"
		}

		result = strings.Replace(result, "?", replaceStr, -1)
	}

	return result, nil
}

func (s *stmt) prepareAndDescribe() error {

	parseMsg := &msgs.FEParseMsg{
		PreparedName: s.preparedName,
		Command:      s.command,
		NumArgs:      0,
	}

	// If we've already been parsed, no reason to do it again.
	if s.parseState == parseStateParsed {
		return nil
	}

	s.parseState = parseStateParseError

	if err := s.conn.sendMessage(parseMsg); err != nil {
		return err
	}

	describeMsg := &msgs.FEDescribeMsg{TargetType: msgs.CmdTargetTypeStatement, TargetName: s.preparedName}

	if err := s.conn.sendMessage(describeMsg); err != nil {
		return err
	}

	if err := s.conn.sendMessage(&msgs.FEFlushMsg{}); err != nil {
		return err
	}

	for {
		bMsg, err := s.conn.recvMessage()

		if err != nil {
			return err
		}

		switch msg := bMsg.(type) {
		case *msgs.BEErrorMsg:
			return msg.ToErrorType()
		case *msgs.BEParseCompleteMsg:
			s.parseState = parseStateParsed
		case *msgs.BERowDescMsg:
			s.lastRowDesc = msg
			return nil
		case *msgs.BENoDataMsg:
			s.lastRowDesc = nil
			return nil
		case *msgs.BEParameterDescMsg:
			s.paramTypes = msg.ParameterTypes
		default:
			s.conn.defaultMessageHandler(msg)
		}
	}
}

func (s *stmt) bindAndExecute(portalName string, args []driver.NamedValue) error {

	// We only need to send the OID types
	paramOIDs := make([]int32, len(s.paramTypes))
	for i, p := range s.paramTypes {
		paramOIDs[i] = int32(p.TypeOID)
	}

	if err := s.conn.sendMessage(&msgs.FEBindMsg{Portal: portalName, Statement: s.preparedName, NamedArgs: args, OIDTypes: paramOIDs}); err != nil {
		return err
	}

	if err := s.conn.sendMessage(&msgs.FEExecuteMsg{Portal: portalName}); err != nil {
		return err
	}

	if err := s.conn.sendMessage(&msgs.FEFlushMsg{}); err != nil {
		return err
	}

	return nil
}

func (s *stmt) collectResults() (*rows, error) {
	rows := emptyRowSet

	if s.lastRowDesc != nil {
		rows = newRows(s.lastRowDesc, s.conn.serverTZOffset)
	}

	for {
		bMsg, err := s.conn.recvMessage()

		if err != nil {
			return emptyRowSet, err
		}

		switch msg := bMsg.(type) {
		case *msgs.BEDataRowMsg:
			rows.addRow(msg)
		case *msgs.BERowDescMsg:
			s.lastRowDesc = msg
			rows = newRows(s.lastRowDesc, s.conn.serverTZOffset)
		case *msgs.BEErrorMsg:
			return emptyRowSet, msg.ToErrorType()
		case *msgs.BEEmptyQueryResponseMsg:
			return emptyRowSet, nil
		case *msgs.BEBindCompleteMsg, *msgs.BECmdDescriptionMsg:
			continue
		case *msgs.BEReadyForQueryMsg, *msgs.BEPortalSuspendedMsg, *msgs.BECmdCompleteMsg:
			return rows, nil
		default:
			_, _ = s.conn.defaultMessageHandler(msg)
		}
	}
}
