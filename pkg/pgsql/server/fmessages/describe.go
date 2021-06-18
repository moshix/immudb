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

type DescribeMsg struct {
	// 'S' to describe a prepared statement; or 'P' to describe a portal.
	descType string
	// The name of the prepared statement or portal to describe (an empty string selects the unnamed prepared statement or portal).
	name string
}

func ParseDescribeMsg(msg []byte) DescribeMsg {
	descType := msg[0]
	name := make([]byte, len(msg)-2)
	copy(name, msg[1:len(msg)-1])
	return DescribeMsg{
		descType: string(descType),
		name:     string(name),
	}
}

func (d *DescribeMsg) GetDescType() string {
	return d.descType
}

func (d *DescribeMsg) GetName() string {
	return d.name
}
