// SPDX-License-Identifier: LGPL-2.1
// Copyright (C) 2021-2022 stu mark

package main

import (
	"os"

	"github.com/nixomose/nixomosegotools/tools"
	slookup_i_lib "github.com/nixomose/slookup_i/slookup_i_lib/slookup_i_interfaces"
	"github.com/nixomose/slookup_i/slookup_i_lib/slookup_i_src"
)

func main() {

	test_basics()

	var log *tools.Nixomosetools_logger = tools.New_Nixomosetools_logger(tools.DEBUG)

	// you can also use the file store, but the problems tend to be in slookup_i not in the physical storage.

	var iopath slookup_i_src.File_store_io_path = slookup_i_src.New_file_store_io_path_default()
	var alignment uint32 = 0

	var data_block_size, block_group_count, addressable_blocks, total_blocks = get_init_params()

	var value_type = make([]byte, 0)
	var dot = make([]byte, 1)
	dot[0] = 0x21
	var n uint32
	for n < data_block_size {
		value_type = append(value_type[:], dot...)
		n++
	}

	var testfile = "/tmp/fstore"
	os.Remove(testfile)

	{ // init the filestore header make it ready to go

		/* so the backing physical store for the slookup is the block device or file passed... */
		var fstore *slookup_i_src.File_store_aligned = slookup_i_src.New_File_store_aligned(log, testfile,
			data_block_size, addressable_blocks, alignment, iopath)

		var tlog = slookup_i_src.New_Tlog(log, fstore, data_block_size, total_blocks)

		var ret tools.Ret
		var slookup *slookup_i_src.Slookup_i = slookup_i_src.New_Slookup_i(log, fstore, tlog,
			addressable_blocks, block_group_count, data_block_size, total_blocks)

		// init the backing store and tlog and slookup header
		ret = slookup.Init()
		if ret != nil {
			return
		}

		ret = slookup.Shutdown()
		if ret != nil {
			return
		}
	} // end init scope

	{

		var fstore *slookup_i_src.File_store_aligned = slookup_i_src.New_File_store_aligned(log, testfile,
			data_block_size, addressable_blocks, alignment, iopath)

		test_4k(log, fstore, &iopath, alignment)

	}

	{
		// test everything again with a memory store.
		var ret tools.Ret
		var mstore *slookup_i_src.Memory_store = slookup_i_src.New_memory_store(log)
		mstore.Init()
		var tlog = slookup_i_src.New_Tlog(log, mstore, data_block_size, total_blocks)
		if ret = tlog.Startup(false); ret != nil {
			return
		}

		test_4k(log, mstore, &iopath, alignment)
	}

}

func get_init_params() (data_block_size uint32, block_group_count uint32, addressabble_blocks uint32, total_blocks uint32) {

	/* data_block_size * block_group_count is the number of bytes in a user facing block, that can be
	for example compressed down to a minimum of 1 data_block size */
	data_block_size = 4096     // bytes in a data block, the fundamental unit of storage in slookup_i
	block_group_count = 5      // a.k.a. additional_nodes_per_block, how many data_blocks are stored in a user-facing 'block'
	addressabble_blocks = 1000 // the number of addressable blocks in this slookup_i table.

	/* the total amount of storable bytes is data_block_size * block_group_count * lookup_table_entry_count */

	/* we could concienvably calculate this by adding up all the space for the header, padding,
	lookup table, padding, tlog, padding, and actual data_block storage. but the idea is that you can
	under provision, and you can just tell us how much space you have and hope you don't run out. */

	total_blocks = addressabble_blocks*block_group_count + 1000
	total_blocks = 500 // for testing let's go with this.

	return
}

func test_4k(log *tools.Nixomosetools_logger, fmstore slookup_i_lib.Slookup_i_backing_store_interface,
	iopath *slookup_i_src.File_store_io_path, alignment uint32) {

	var ret tools.Ret
	var data_block_size uint32
	var block_group_count uint32
	var addressable_blocks uint32
	var total_blocks uint32
	data_block_size, block_group_count, addressable_blocks, total_blocks = get_init_params()

	var tlog = slookup_i_src.New_Tlog(log, fmstore, data_block_size, total_blocks)
	if ret = tlog.Startup(false); ret != nil {
		return
	}

	var slookup_i *slookup_i_src.Slookup_i = slookup_i_src.New_Slookup_i(log, fmstore, tlog, addressable_blocks,
		block_group_count, data_block_size, total_blocks)

	ret = slookup_i.Startup(false)
	if ret != nil {
		tools.Error(log, "Unable to create slookup: ", ret.Get_errmsg())
		return
	}

	var lib slookup_i_test_lib = New_slookup_i_test_lib(log)
	lib.Slookup_4k_tests(slookup_i)
	slookup_i.Shutdown()

}

func test_basics() {

	var ret tools.Ret
	var loggertest *tools.Nixomosetools_logger = tools.New_Nixomosetools_logger(tools.DEBUG)

	loggertest.Set_level(tools.INFO)

	loggertest.Debug("hi from stu")
	loggertest.Info("hi from stu", " a second logger param", tools.Inttostring(749))
	loggertest.Error("hi from stu")

	loggertest.Set_level(tools.DEBUG)

	// you can also use the file store, but the problems tend to be in slookup_i not in the physical storage.
	var mstore *slookup_i_src.Memory_store = slookup_i_src.New_memory_store(loggertest)
	var data_block_size, block_group_count, addressable_blocks, total_blocks = get_init_params()
	var tlog = slookup_i_src.New_Tlog(loggertest, mstore, data_block_size, total_blocks)

	var slookup *slookup_i_src.Slookup_i = slookup_i_src.New_Slookup_i(loggertest, mstore, tlog,
		addressable_blocks, block_group_count, data_block_size, total_blocks)

	ret = slookup.Startup(false)
	if ret != nil {
		tools.Error(loggertest, "Unable to create block storage: ", ret.Get_errmsg())
		return
	}
	ret = slookup.Shutdown()
	if ret != nil {
		tools.Error(loggertest, "Unable to shut down block storage: ", ret.Get_errmsg())
		return
	}

}
