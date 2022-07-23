// SPDX-License-Identifier: LGPL-2.1
// Copyright (C) 2021-2022 stu mark

package slookup_i_src


import (
	"os"

	"github.com/ncw/directio"
)

/* the interface you must implement to read or write to files/devices.
   this way we can inject a default io path or a directio path */

type File_store_io_path interface {
	OpenFile(name string, flag int, perm os.FileMode) (*os.File, error)
	AllocBuffer(size int) []byte
}

/************************************************************************************/

var _ File_store_io_path = &File_store_io_path_default{}
var _ File_store_io_path = (*File_store_io_path_default)(nil)

var _ File_store_io_path = &File_store_io_path_directio{}
var _ File_store_io_path = (*File_store_io_path_directio)(nil)

/************************************************************************************/

type File_store_io_path_default struct {
}

func New_file_store_io_path_default() *File_store_io_path_default {
	return &File_store_io_path_default{}
}

func (this *File_store_io_path_default) OpenFile(name string, flag int, perm os.FileMode) (*os.File, error) {
	return os.OpenFile(name, flag, perm)
}

func (this *File_store_io_path_default) AllocBuffer(size int) []byte {
	var block []byte = make([]byte, size)
	return block
}

/************************************************************************************/

type File_store_io_path_directio struct {
}

func New_file_store_io_path_directio() *File_store_io_path_directio {
	return &File_store_io_path_directio{}
}

func (this *File_store_io_path_directio) OpenFile(name string, flag int, perm os.FileMode) (*os.File, error) {
	return directio.OpenFile(name, flag, perm)
}

func (this *File_store_io_path_directio) AllocBuffer(size int) []byte {
	// round up from the size they want to the next directio blocksize
	/* we can't supply a buffer shorter than the directio block size
	or the kernel will write past the end of our buffer, we must supply a
	fully padded out block of memory for it to read into, the caller
	can shorted the go slice to whatever it wants, but the read
	must be fully padded */
	var blocks = size / directio.BlockSize
	if size%directio.BlockSize != 0 {
		blocks++
	}
	var block []byte = directio.AlignedBlock(blocks * directio.BlockSize)
	return block
}
