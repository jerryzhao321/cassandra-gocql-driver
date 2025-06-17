/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
/*
 * Content before git sha 34fdeebefcbf183ed7f916f931aa0586fdaa1b40
 * Copyright (c) 2012, The Gocql authors,
 * provided under the BSD-3-Clause License.
 * See the NOTICE file distributed with this work for additional information.
 */

package gocql

import (
	"bytes"
	"fmt"
	"net"
	"reflect"
)

// RowData contains the column names and pointers to the default values for each
// column
type RowData struct {
	Columns []string
	Values  []interface{}
}

func dereference(i interface{}) interface{} {
	return reflect.Indirect(reflect.ValueOf(i)).Interface()
}

// TupeColumnName will return the column name of a tuple value in a column named
// c at index n. It should be used if a specific element within a tuple is needed
// to be extracted from a map returned from SliceMap or MapScan.
func TupleColumnName(c string, n int) string {
	return fmt.Sprintf("%s[%d]", c, n)
}

// RowData returns the RowData for the iterator.
func (iter *Iter) RowData() (RowData, error) {
	if iter.err != nil {
		return RowData{}, iter.err
	}

	columns := make([]string, 0, len(iter.Columns()))
	values := make([]interface{}, 0, len(iter.Columns()))

	for _, column := range iter.Columns() {
		if c, ok := column.TypeInfo.(TupleTypeInfo); !ok {
			val := c.Zero()
			columns = append(columns, column.Name)
			values = append(values, &val)
		} else {
			for i, elem := range c.Elems {
				columns = append(columns, TupleColumnName(column.Name, i))
				var val interface{}
				val = elem.Zero()
				values = append(values, &val)
			}
		}
	}

	rowData := RowData{
		Columns: columns,
		Values:  values,
	}

	return rowData, nil
}

// SliceMap is a helper function to make the API easier to use
// returns the data from the query in the form of []map[string]interface{}
func (iter *Iter) SliceMap() ([]map[string]interface{}, error) {
	if iter.err != nil {
		return nil, iter.err
	}

	numCols := len(iter.Columns())
	var dataToReturn []map[string]interface{}
	for {
		m := make(map[string]interface{}, numCols)
		if !iter.MapScan(m) {
			break
		}
		dataToReturn = append(dataToReturn, m)
	}
	if iter.err != nil {
		return nil, iter.err
	}
	return dataToReturn, nil
}

// MapScan takes a map[string]interface{} and populates it with a row
// that is returned from cassandra.
//
// Each call to MapScan() must be called with a new map object.
// During the call to MapScan() any pointers in the existing map
// are replaced with non pointer types before the call returns
//
//	iter := session.Query(`SELECT * FROM mytable`).Iter()
//	for {
//		// New map each iteration
//		row := make(map[string]interface{})
//		if !iter.MapScan(row) {
//			break
//		}
//		// Do things with row
//		if fullname, ok := row["fullname"]; ok {
//			fmt.Printf("Full Name: %s\n", fullname)
//		}
//	}
//
// You can also pass pointers in the map before each call
//
//	var fullName FullName // Implements gocql.Unmarshaler and gocql.Marshaler interfaces
//	var address net.IP
//	var age int
//	iter := session.Query(`SELECT * FROM scan_map_table`).Iter()
//	for {
//		// New map each iteration
//		row := map[string]interface{}{
//			"fullname": &fullName,
//			"age":      &age,
//			"address":  &address,
//		}
//		if !iter.MapScan(row) {
//			break
//		}
//		fmt.Printf("First: %s Age: %d Address: %q\n", fullName.FirstName, age, address)
//	}
func (iter *Iter) MapScan(m map[string]interface{}) bool {
	if iter.err != nil {
		return false
	}

	cols := iter.Columns()
	columnNames := make([]string, 0, len(cols))
	values := make([]interface{}, 0, len(cols))
	for _, column := range iter.Columns() {
		if c, ok := column.TypeInfo.(TupleTypeInfo); ok {
			for i := range c.Elems {
				columnName := TupleColumnName(column.Name, i)
				if dest, ok := m[columnName]; ok {
					values = append(values, dest)
				} else {
					zero := c.Elems[i].Zero()
					// technically this is a *interface{} but later we will fix it
					values = append(values, &zero)
				}
				columnNames = append(columnNames, columnName)
			}
		} else {
			if dest, ok := m[column.Name]; ok {
				values = append(values, dest)
			} else {
				zero := column.TypeInfo.Zero()
				// technically this is a *interface{} but later we will fix it
				values = append(values, &zero)
			}
			columnNames = append(columnNames, column.Name)
		}
	}
	if iter.Scan(values...) {
		for i, name := range columnNames {
			if iptr, ok := values[i].(*interface{}); ok {
				m[name] = *iptr
			} else {
				// TODO: it seems wrong to dereference the values that were passed in
				// originally in the map but that's what it was doing before
				m[name] = dereference(values[i])
			}
		}
		return true
	}
	return false
}

func copyBytes(p []byte) []byte {
	b := make([]byte, len(p))
	copy(b, p)
	return b
}

var failDNS = false

func LookupIP(host string) ([]net.IP, error) {
	if failDNS {
		return nil, &net.DNSError{}
	}
	return net.LookupIP(host)

}

func ringString(hosts []*HostInfo) string {
	buf := new(bytes.Buffer)
	for _, h := range hosts {
		buf.WriteString("[" + h.ConnectAddress().String() + "-" + h.HostID() + ":" + h.State().String() + "]")
	}
	return buf.String()
}
