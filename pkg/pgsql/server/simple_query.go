/*
Copyright 2021 CodeNotary, Inc. All rights reserved.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

	http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package server

import (
	"errors"
	"github.com/codenotary/immudb/embedded/sql"
	"github.com/codenotary/immudb/pkg/api/schema"
	bm "github.com/codenotary/immudb/pkg/pgsql/server/bmessages"
	fm "github.com/codenotary/immudb/pkg/pgsql/server/fmessages"
	"github.com/codenotary/immudb/pkg/pgsql/server/pgmeta"
	"io"
	"regexp"
	"strings"
)

// HandleSimpleQueries errors are returned and handled in the caller
func (s *session) HandleSimpleQueries() (err error) {
	s.Lock()
	defer s.Unlock()
	for {
		if _, err := s.writeMessage(bm.ReadyForQuery()); err != nil {
			return err
		}
		msg, err := s.nextMessage()
		if err != nil {
			if err == io.EOF {
				s.log.Warningf("connection is closed")
				return nil
			}
			s.ErrorHandle(err)
			continue
		}

		switch v := msg.(type) {
		case fm.TerminateMsg:
			return s.mr.CloseConnection()
		case fm.QueryMsg:
			var set = regexp.MustCompile(`(?i)set\s+.+`)
			if set.MatchString(v.GetStatements()) {
				if _, err := s.writeMessage(bm.CommandComplete([]byte(`ok`))); err != nil {
					s.ErrorHandle(err)
				}
				continue
			}
			var version = regexp.MustCompile(`(?i)select\s+version\(\s*\)`)
			if version.MatchString(v.GetStatements()) {
				if err = s.writeVersionInfo(); err != nil {
					s.ErrorHandle(err)
					continue
				}
				continue
			}
			// todo handle the result outside in order to avoid err suppression
			if _, err = s.queryMsg(v.GetStatements()); err != nil {
				s.ErrorHandle(err)
				continue
			}
		case fm.ParseMsg:
			var res *schema.SQLQueryResult
			stmts, err := sql.Parse(strings.NewReader(v.GetStatements()))
			if err != nil {
				return err
			}
			for _, stmt := range stmts {
				switch st := stmt.(type) {
				case *sql.SelectStmt:
					res, err = s.database.SQLQueryPrepared(st, nil, true)
					if err != nil {
						return err
					}
				default:
					return errors.New("errore")
				}

				// Parse
				if _, err = s.writeMessage(bm.ParseComplete()); err != nil {
					s.ErrorHandle(err)
					continue
				}
				// describe
				msg, err := s.nextMessage()
				if err != nil {
					if err == io.EOF {
						s.log.Warningf("connection is closed")
						return nil
					}
					s.ErrorHandle(err)
					continue
				}
				if describe, ok := msg.(fm.DescribeMsg); ok == false {
					continue
				} else {
					// The Describe message (statement variant) specifies the name of an existing prepared statement
					// (or an empty string for the unnamed prepared statement). The response is a ParameterDescription
					// message describing the parameters needed by the statement, followed by a RowDescription message
					// describing the rows that will be returned when the statement is eventually executed (or a NoData
					// message if the statement will not return rows). ErrorResponse is issued if there is no such prepared
					// statement. Note that since Bind has not yet been issued, the formats to be used for returned columns
					// are not yet known to the backend; the format code fields in the RowDescription message will be zeroes
					// in this case.
					if describe.GetDescType() == "S" {
						if _, err = s.writeMessage(bm.ParameterDescriptiom(len(res.Columns))); err != nil {
							s.ErrorHandle(err)
							continue
						}
						if _, err := s.writeMessage(bm.RowDescription(res.Columns)); err != nil {
							s.ErrorHandle(err)
						}
					}
					// The Describe message (portal variant) specifies the name of an existing portal (or an empty string
					// for the unnamed portal). The response is a RowDescription message describing the rows that will be
					// returned by executing the portal; or a NoData message if the portal does not contain a query that
					// will return rows; or ErrorResponse if there is no such portal.
					if describe.GetDescType() == "P" {
						if _, err = s.writeMessage(bm.ParseComplete()); err != nil {
							s.ErrorHandle(err)
							continue
						}
					}
				}
				// sync
				msg, err = s.nextMessage()
				if err != nil {
					if err == io.EOF {
						s.log.Warningf("connection is closed")
						return nil
					}
					s.ErrorHandle(err)
					continue
				}
				println(msg)
				msg, err = s.nextMessage()
				if err != nil {
					if err == io.EOF {
						s.log.Warningf("connection is closed")
						return nil
					}
					s.ErrorHandle(err)
					continue
				}
			}
			println(msg)

		default:
			s.ErrorHandle(ErrUnknowMessageType)
			continue
		}
		if _, err := s.writeMessage(bm.CommandComplete([]byte(`ok`))); err != nil {
			s.ErrorHandle(err)
			continue
		}
	}
}

func (s *session) queryMsg(statements string) (*schema.SQLExecResult, error) {
	var res *schema.SQLExecResult
	stmts, err := sql.Parse(strings.NewReader(statements))
	if err != nil {
		return nil, err
	}
	for _, stmt := range stmts {
		switch st := stmt.(type) {
		case *sql.UseDatabaseStmt:
			{
				return nil, ErrUseDBStatementNotSupported
			}
		case *sql.CreateDatabaseStmt:
			{
				return nil, ErrCreateDBStatementNotSupported
			}
		case *sql.SelectStmt:
			err := s.selectStatement(st)
			if err != nil {
				return nil, err
			}
		case sql.SQLStmt:
			res, err = s.database.SQLExecPrepared([]sql.SQLStmt{st}, nil, true)
			if err != nil {
				return nil, err
			}
		}
	}
	return res, nil
}

func (s *session) selectStatement(st *sql.SelectStmt) error {
	res, err := s.database.SQLQueryPrepared(st, nil, true)
	if err != nil {
		return err
	}
	if res != nil && len(res.Rows) > 0 {
		if _, err = s.writeMessage(bm.RowDescription(res.Columns)); err != nil {
			return err
		}
		if _, err = s.writeMessage(bm.DataRow(res.Rows, len(res.Columns), false)); err != nil {
			return err
		}
		return nil
	}
	if _, err = s.writeMessage(bm.EmptyQueryResponse()); err != nil {
		return err
	}
	return nil
}

func (s *session) writeVersionInfo() error {
	cols := []*schema.Column{{Name: "version", Type: "VARCHAR"}}
	if _, err := s.writeMessage(bm.RowDescription(cols)); err != nil {
		return err
	}
	rows := []*schema.Row{{
		Columns: []string{"version"},
		Values:  []*schema.SQLValue{{Value: &schema.SQLValue_S{S: pgmeta.PgsqlProtocolVersionMessage}}},
	}}
	if _, err := s.writeMessage(bm.DataRow(rows, len(cols), false)); err != nil {
		return err
	}
	if _, err := s.writeMessage(bm.CommandComplete([]byte(`ok`))); err != nil {
		s.ErrorHandle(err)
	}
	return nil
}
