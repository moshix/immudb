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
package multiapp

import (
	"io"
	"os"
	"path/filepath"
	"testing"

	"github.com/codenotary/immudb/embedded/appendable"
	"github.com/codenotary/immudb/embedded/appendable/singleapp"

	"github.com/stretchr/testify/require"
)

func TestMultiApp(t *testing.T) {
	md := appendable.NewMetadata(nil)
	md.PutInt("mkey1", 1)

	a, err := Open("testdata", DefaultOptions().WithMetadata(md.Bytes()))
	defer os.RemoveAll("testdata")
	require.NoError(t, err)

	sz, err := a.Size()
	require.NoError(t, err)
	require.Equal(t, int64(0), sz)

	require.Equal(t, appendable.DefaultCompressionFormat, a.CompressionFormat())
	require.Equal(t, appendable.DefaultCompressionLevel, a.CompressionLevel())

	err = a.SetOffset(0)
	require.NoError(t, err)

	require.Equal(t, int64(0), a.Offset())

	mkey1, found := appendable.NewMetadata(a.Metadata()).GetInt("mkey1")
	require.True(t, found)
	require.Equal(t, 1, mkey1)

	_, _, err = a.Append(nil)
	require.Equal(t, ErrIllegalArguments, err)

	_, _, err = a.Append([]byte{})
	require.Equal(t, ErrIllegalArguments, err)

	off, n, err := a.Append([]byte{0})
	require.NoError(t, err)
	require.Equal(t, int64(0), off)
	require.Equal(t, 1, n)

	off, n, err = a.Append([]byte{1, 2, 3})
	require.NoError(t, err)
	require.Equal(t, int64(1), off)
	require.Equal(t, 3, n)

	off, n, err = a.Append([]byte{4, 5, 6, 7, 8, 9, 10})
	require.NoError(t, err)
	require.Equal(t, int64(4), off)
	require.Equal(t, 7, n)

	err = a.Flush()
	require.NoError(t, err)

	bs := make([]byte, 4)
	n, err = a.ReadAt(bs, 0)
	require.NoError(t, err)
	require.Equal(t, []byte{0, 1, 2, 3}, bs)

	bs = make([]byte, 4)
	n, err = a.ReadAt(bs, 7)
	require.NoError(t, err)
	require.Equal(t, []byte{7, 8, 9, 10}, bs)

	err = a.Sync()
	require.NoError(t, err)

	err = a.Close()
	require.NoError(t, err)
}

func TestMultiApOffsetAndLRUCacheEviction(t *testing.T) {
	a, err := Open("testdata", DefaultOptions().WithFileSize(1).WithMaxOpenedFiles(1))
	defer os.RemoveAll("testdata")
	require.NoError(t, err)

	off, n, err := a.Append([]byte{0, 1, 2, 3, 4, 5, 6, 7})
	require.NoError(t, err)

	err = a.Flush()
	require.NoError(t, err)

	err = a.SetOffset(0)
	require.NoError(t, err)

	_, _, err = a.Append([]byte{7, 6, 5, 4, 3, 2, 1, 0})
	require.NoError(t, err)

	err = a.Flush()
	require.NoError(t, err)

	b := make([]byte, n)
	_, err = a.ReadAt(b, off)
	require.NoError(t, err)

	require.Equal(t, []byte{7, 6, 5, 4, 3, 2, 1, 0}, b)
}

func TestMultiAppClosedAndDeletedFiles(t *testing.T) {
	a, err := Open("testdata", DefaultOptions().WithFileSize(1).WithMaxOpenedFiles(1))
	defer os.RemoveAll("testdata")
	require.NoError(t, err)

	_, n, err := a.Append([]byte{0, 1, 2, 3, 4, 5, 6, 7})
	require.NoError(t, err)

	err = a.appendables.Apply(func(k interface{}, v interface{}) error {
		return v.(*singleapp.AppendableFile).Close()
	})
	require.NoError(t, err)

	err = a.Close()
	require.Equal(t, singleapp.ErrAlreadyClosed, err)

	fname := filepath.Join(a.path, appendableName(0, a.fileExt))
	os.Remove(fname)

	a, err = Open("testdata", DefaultOptions().WithFileSize(1).WithMaxOpenedFiles(1))
	require.NoError(t, err)

	b := make([]byte, n)
	_, err = a.ReadAt(b, 0)
	require.Equal(t, io.EOF, err)
}

func TestMultiAppClosedFiles(t *testing.T) {
	a, err := Open("testdata", DefaultOptions().WithFileSize(1).WithMaxOpenedFiles(2))
	defer os.RemoveAll("testdata")
	require.NoError(t, err)

	_, _, err = a.Append([]byte{0, 1, 2})
	require.NoError(t, err)

	err = a.appendables.Apply(func(k interface{}, v interface{}) error {
		return v.(*singleapp.AppendableFile).Close()
	})
	require.NoError(t, err)

	_, _, err = a.Append([]byte{3, 4, 5})
	require.Equal(t, singleapp.ErrAlreadyClosed, err)
}

func TestMultiAppReOpening(t *testing.T) {
	a, err := Open("testdata", DefaultOptions().WithFileSize(1))
	defer os.RemoveAll("testdata")
	require.NoError(t, err)

	off, n, err := a.Append([]byte{1, 2})
	require.NoError(t, err)
	require.Equal(t, int64(0), off)
	require.Equal(t, 2, n)

	off, n, err = a.Append([]byte{3})
	require.NoError(t, err)
	require.Equal(t, int64(2), off)
	require.Equal(t, 1, n)

	err = a.Copy("testdata_copy")
	require.NoError(t, err)

	defer os.RemoveAll("testdata_copy")

	err = a.Close()
	require.NoError(t, err)

	a, err = Open("testdata_copy", DefaultOptions().WithReadOnly(true))
	require.NoError(t, err)

	sz, err := a.Size()
	require.NoError(t, err)
	require.Equal(t, int64(3), sz)

	bs := make([]byte, 3)
	n, err = a.ReadAt(bs, 0)
	require.NoError(t, err)
	require.Equal(t, []byte{1, 2, 3}, bs)

	_, _, err = a.Append([]byte{})
	require.Equal(t, ErrReadOnly, err)

	_, _, err = a.Append([]byte{0})
	require.Equal(t, ErrReadOnly, err)

	err = a.Flush()
	require.Equal(t, ErrReadOnly, err)

	err = a.Sync()
	require.Equal(t, ErrReadOnly, err)

	err = a.Close()
	require.NoError(t, err)
}

func TestMultiAppEdgeCases(t *testing.T) {
	_, err := Open("testdata", nil)
	require.Equal(t, ErrIllegalArguments, err)

	_, err = Open("multi_app_test.go", DefaultOptions())
	require.Equal(t, ErrorPathIsNotADirectory, err)

	_, err = Open("testdata", DefaultOptions().WithReadOnly(true))
	require.Error(t, err)

	a, err := Open("testdata", DefaultOptions())
	defer os.RemoveAll("testdata")
	require.NoError(t, err)

	_, err = a.ReadAt(nil, 0)
	require.Equal(t, ErrIllegalArguments, err)

	_, err = a.ReadAt([]byte{}, 0)
	require.Equal(t, ErrIllegalArguments, err)

	err = a.Copy("multi_app_test.go")
	require.Error(t, err)
	require.Contains(t, err.Error(), "not a directory")

	err = a.Close()
	require.NoError(t, err)

	_, err = a.Size()
	require.Equal(t, ErrAlreadyClosed, err)

	err = a.Copy("copy")
	require.Equal(t, ErrAlreadyClosed, err)

	err = a.SetOffset(0)
	require.Equal(t, ErrAlreadyClosed, err)

	_, _, err = a.Append([]byte{})
	require.Equal(t, ErrAlreadyClosed, err)

	_, err = a.ReadAt(make([]byte, 1), 0)
	require.Equal(t, ErrAlreadyClosed, err)

	err = a.Flush()
	require.Equal(t, ErrAlreadyClosed, err)

	err = a.Sync()
	require.Equal(t, ErrAlreadyClosed, err)

	err = a.Close()
	require.Equal(t, ErrAlreadyClosed, err)
}

func TestMultiAppCompression(t *testing.T) {
	a, err := Open("testdata", DefaultOptions().WithCompressionFormat(appendable.ZLibCompression))
	defer os.RemoveAll("testdata")
	require.NoError(t, err)

	off, _, err := a.Append([]byte{1, 2, 3})
	require.NoError(t, err)
	require.Equal(t, int64(0), off)

	err = a.Flush()
	require.NoError(t, err)

	bs := make([]byte, 3)
	_, err = a.ReadAt(bs, 0)
	require.NoError(t, err)
	require.Equal(t, []byte{1, 2, 3}, bs)

	err = a.Close()
	require.NoError(t, err)
}

func TestMultiAppAppendableForCurrentChunk(t *testing.T) {
	a, err := Open("testdata", DefaultOptions().WithFileSize(10))
	defer os.RemoveAll("testdata")
	require.NoError(t, err)

	testData := []byte{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12}

	off, n, err := a.Append(testData)
	require.NoError(t, err)
	require.EqualValues(t, 0, off)
	require.EqualValues(t, n, 12)

	app, err := a.appendableFor(11)
	require.NoError(t, err)
	require.Equal(t, a.currApp, app)
}
