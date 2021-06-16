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

package fmessages

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"log"
)

type ParseMsg struct {
	paramsCount int16
	name        []byte
	statements  string
	objectIDs   []int32
}

func ParseParseMsg(msg []byte) ParseMsg {
	/*
		String
		The name of the destination prepared statement (an empty string selects the unnamed prepared statement).

		String
		The query string to be parsed.

		Int16
		The number of parameter data types specified (can be zero). Note that this is not an indication of the number of parameters that might appear in the query string, only the number that the frontend wants to prespecify types for.

		Then, for each parameter, there is the following:

		Int32
		Specifies the object ID of the parameter data type. Placing a zero here is equivalent to leaving the type unspecified.
	*/

	b := bytes.NewBuffer(msg)
	r := bufio.NewReader(b)
	name, err := r.ReadBytes(0)
	if err != nil {
		log.Fatal(err)
	}
	queryString, err := r.ReadBytes(0)
	if err != nil {
		log.Fatal(err)
	}

	pcb := make([]byte, 4)
	_, err = r.Read(pcb)
	if err != nil {
		log.Fatal(err)
	}
	pCount := binary.BigEndian.Uint16(pcb)

	objectIDs := make([]int32, 0)
	IDs := make([]byte, 8)
	for _, err := r.Read(pcb); err == nil; {
		objectIDs = append(objectIDs, int32(binary.BigEndian.Uint32(IDs)))
	}

	return ParseMsg{name: name, statements: string(queryString), paramsCount: int16(pCount), objectIDs: objectIDs}
}

func (q *ParseMsg) GetStatements() string {
	return q.statements
}
