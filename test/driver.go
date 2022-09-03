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

	var log *tools.Nixomosetools_logger = tools.New_Nixomosetools_logger(tools.DEBUG)

	// you can also use the file store, but the problems tend to be in slookup_i not in the physical storage.

	var iopath = slookup_i_src.New_file_store_io_path_default()

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

		// var device = lib.New_block_device("testdevice", 1024*1024*1024,
		// 	testfile, false, false, 0, VALUE_LENGTH, 0, 0, false, "", false, false)

		var iopath slookup_i_src.File_store_io_path = slookup_i_src.New_file_store_io_path_default()
		var alignment uint32 = 0

		/* so the backing physical store for the stree is the block device or file passed... */
		var fstore *slookup_i_src.File_store_aligned = slookup_i_src.New_File_store_aligned(log, testfile,
			data_block_size, addressable_blocks, alignment, iopath)

		var tlog = slookup_i_src.New_Tlog(log, fstore, data_block_size, total_blocks)
		var ret tools.Ret
		if ret = tlog.Init(); ret != nil {
			return
		}

		var slookup *slookup_i_src.Slookup_i = slookup_i_src.New_Slookup_i(log, fstore, tlog,
			addressable_blocks, block_group_count, data_block_size, total_blocks)

		// init the backing store
		ret = slookup.Init()
		if ret != nil {
			return
		}

		ret = slookup.Shutdown()
		if ret != nil {
			return
		}
	} // end init scope

	var alignment uint32 = 0
	// now make one we can test with.
	var fstore *slookup_i_src.File_store_aligned = slookup_i_src.New_File_store_aligned(log, testfile,
		data_block_size, addressable_blocks, alignment, iopath)

	var ret = fstore.Open_datastore()
	if ret != nil {
		return
	}

	ret = fstore.Load_header_and_check_magic(true) // check device params passed in from cmd line or catalog
	if ret != nil {
		return
	}

	// stree has to be unstarted for test to run
	ret = fstore.Shutdown()
	if ret != nil {
		return
	}

	var tlog = slookup_i_src.New_Tlog(log, fstore, data_block_size, total_blocks)
	if ret = tlog.Startup(false); ret != nil {
		return
	}

	test_4k(log, tlog, fstore)

	var mstore *slookup_i_src.Memory_store = slookup_i_src.New_memory_store(log)
	mstore.Init()

	test_4k(log, tlog, mstore)

	test_basics()

	test_tree_alone()
	test_tree_and_offspring()

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

func test_4k(log *tools.Nixomosetools_logger, tlog slookup_i_lib.Transaction_log_interface, store slookup_i_lib.Slookup_i_backing_store_interface) {

	var data_block_size uint32
	var block_group_count uint32
	var addressable_blocks uint32
	var total_blocks uint32
	data_block_size, block_group_count, addressable_blocks, total_blocks = get_init_params()

	var slookup_i *slookup_i_src.Slookup_i = slookup_i_src.New_Slookup_i(log, store, tlog, addressable_blocks,
		block_group_count, data_block_size, total_blocks)

	var ret = slookup_i.Startup(false)
	if ret != nil {
		tools.Error(log, "Unable to create stree: ", ret.Get_errmsg())
		return
	}

	var lib slookup_i_test_lib = New_slookup_i_test_lib(log)
	lib.Stree_4k_tests(slookup_i, KEY_LENGTH, VALUE_LENGTH)
	slookup_i.Shutdown()

}

func test_basics() {

	var loggertest *tools.Nixomosetools_logger = tools.New_Nixomosetools_logger(tools.DEBUG)

	loggertest.Set_level(tools.INFO)

	loggertest.Debug("hi from stu")
	loggertest.Info("hi from stu", " a second logger param", tools.Inttostring(749))
	loggertest.Error("hi from stu")

	loggertest.Set_level(tools.DEBUG)

	// you can also use the file store, but the problems tend to be in slookup_i not in the physical storage.
	var mstore *slookup_i_lib.Memory_store = slookup_i_lib.New_memory_store(loggertest)

	var max_key_len uint32 = 20
	var max_value_len uint32 = 40
	var additional_nodes_per_block uint32 = 0 // you always get one node, this is how many ADDITIONAL nodes you want.

	var ret, block_size = slookup_i_lib.Calculate_block_size(loggertest, "key", []byte("value"), max_key_len, max_value_len, additional_nodes_per_block)

	var slookup_i *slookup_i_lib.slookup_i = slookup_i_lib.New_slookup_i(loggertest, mstore, max_key_len,
		max_value_len, additional_nodes_per_block, block_size, "sameple_key", []byte("sample_value"))

	ret = slookup_i.Startup(false)
	if ret != nil {
		tools.Error(loggertest, "Unable to create block storage: ", ret.Get_errmsg())
		return
	}

}

func test_tree_and_offspring() {

	var log *tools.Nixomosetools_logger = tools.New_Nixomosetools_logger(tools.DEBUG)

	var KEY_LENGTH uint32 = 2
	var VALUE_LENGTH uint32 = 4 // this is how much data you can store in one node

	var additional_nodes_per_block uint32 = 0 // you always get one node, this is how many ADDITIONAL nodes you want.
	additional_nodes_per_block = 4            // this is how many nodes there are in one block referred to by a single key minus one, because you always get one.
	var mstore *slookup_i_lib.Memory_store = slookup_i_lib.New_memory_store(log)

	// this is the key they offspring nodes will get by default.

	var key_type string = ("--")
	var value_type []byte = []byte("****")

	var ret, block_size = slookup_i_lib.Calculate_block_size(log, key_type, value_type, KEY_LENGTH, VALUE_LENGTH, additional_nodes_per_block)

	var slookup_i *slookup_i_lib.slookup_i = slookup_i_lib.New_slookup_i(log, mstore, KEY_LENGTH,
		VALUE_LENGTH, additional_nodes_per_block, block_size, key_type, value_type)

	ret = slookup_i.Startup(false)
	if ret != nil {
		tools.Error(log, "Unable to create block storage: ", ret.Get_errmsg())
		return
	}

	var lib slookup_i_test_lib = New_slookup_i_test_lib(log)
	lib.Stree_test_offspring(slookup_i, KEY_LENGTH, VALUE_LENGTH, additional_nodes_per_block)
	slookup_i.Shutdown()
}

func test_tree_alone() {

	var log *tools.Nixomosetools_logger = tools.New_Nixomosetools_logger(tools.DEBUG)

	var KEY_LENGTH uint32 = 2
	var VALUE_LENGTH uint32 = 4 // this is how much data you can store in one node

	var additional_nodes_per_block uint32 = 0 // you always get one node, this is how many ADDITIONAL nodes you want.
	additional_nodes_per_block = 4            // this is how many nodes there are in one block referred to by a single key minus one, because you always get one.
	var mstore *slookup_i_lib.Memory_store = slookup_i_lib.New_memory_store(log)

	var key_type string = ("defkey")
	var value_type []byte = []byte("defvalue")

	var ret, block_size = slookup_i_lib.Calculate_block_size(log, key_type, value_type, KEY_LENGTH, VALUE_LENGTH, additional_nodes_per_block)

	var slookup_i *slookup_i_lib.slookup_i = slookup_i_lib.New_slookup_i(log, mstore, KEY_LENGTH,
		VALUE_LENGTH, additional_nodes_per_block, block_size, key_type, value_type)

	ret = slookup_i.Startup(false)
	if ret != nil {
		tools.Error(log, "Unable to create block storage: ", ret.Get_errmsg())
		return
	}

	var lib slookup_i_test_lib = New_slookup_i_test_lib(log)

	lib.Stree_test_run(slookup_i, KEY_LENGTH, VALUE_LENGTH)
	slookup_i.Shutdown()
}
