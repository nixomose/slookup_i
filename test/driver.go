// SPDX-License-Identifier: LGPL-2.1
// Copyright (C) 2021-2022 stu mark

package main

import (
	"os"

	"github.com/nixomose/nixomosegotools/tools"
	stree_v_interfaces "github.com/nixomose/stree_v/stree_v_lib/stree_v_interfaces"
	"github.com/nixomose/stree_v/stree_v_lib/stree_v_lib"
)

func main() {

	var log *tools.Nixomosetools_logger = tools.New_Nixomosetools_logger(tools.DEBUG)

	// you can also use the file store, but the problems tend to be in stree_v not in the physical storage.

	var iopath = stree_v_lib.New_file_store_io_path_default()

	var KEY_LENGTH, VALUE_LENGTH, _, _, additional_nodes_per_block = get_init_params()
	var key_type string = ""
	var n uint32 = 0
	for n < KEY_LENGTH {
		key_type += "."
		n++
	}
	n = 0
	var value_type = make([]byte, 0)
	var dot = make([]byte, 1)
	dot[0] = 0x21
	for n < VALUE_LENGTH {
		value_type = append(value_type[:], dot...)
		n++
	}

	/* this is how much data in a single unit stree is going to ask the backing store to store.
	it's the size of the actual block of data we want to store plus the stree header,
	this is also called the stree_block_size */
	var ret, stree_calculated_node_size = stree_v_lib.Calculate_block_size(log, key_type, value_type, KEY_LENGTH, VALUE_LENGTH, uint32(additional_nodes_per_block))
	if ret != nil {
		return
	}

	var testfile = "/tmp/fstore"
	os.Remove(testfile)

	{ // init the filestore header make it ready to go

		// var device = lib.New_block_device("testdevice", 1024*1024*1024,
		// 	testfile, false, false, 0, VALUE_LENGTH, 0, 0, false, "", false, false)

		var iopath stree_v_lib.File_store_io_path = stree_v_lib.New_file_store_io_path_default()

		/* so the backing physical store for the stree is the block device or file passed... */
		var fstore *stree_v_lib.File_store_aligned = stree_v_lib.New_File_store_aligned(log,
			testfile, uint32(stree_calculated_node_size), uint32(stree_calculated_node_size),
			additional_nodes_per_block, iopath)

		var stree *stree_v_lib.Stree_v = stree_v_lib.New_Stree_v(log, fstore, KEY_LENGTH, VALUE_LENGTH,
			additional_nodes_per_block, stree_calculated_node_size, "", []byte(""))

		ret = stree.Init()
		if ret != nil {
			return
		}

		ret = stree.Shutdown()
		if ret != nil {
			return
		}
	}
	var fstore *stree_v_lib.File_store_aligned = stree_v_lib.New_File_store_aligned(log, testfile,
		stree_calculated_node_size, stree_calculated_node_size, additional_nodes_per_block, iopath)

	ret = fstore.Open_datastore()
	if ret != nil {
		return
	}

	ret = fstore.Load_header_and_check_magic(true) // check device params passed in from cmd line or catalog
	if ret != nil {
		return
	}
	// we did this above
	// ret = fstore.Init()
	// if ret != nil {
	// 	return
	// }
	// stree has to be unstarted for test to run
	ret = fstore.Shutdown()
	if ret != nil {
		return
	}

	test_4k(log, fstore)

	var mstore *stree_v_lib.Memory_store = stree_v_lib.New_memory_store(log)
	mstore.Init()

	test_4k(log, mstore)

	test_basics()

	test_tree_alone()
	test_tree_and_offspring()

}

func get_init_params() (uint32, uint32, uint32, uint32, uint32) {

	var sample_key = Key_from_block_num(0)
	var KEY_LENGTH uint32 = uint32(len(sample_key))
	var VALUE_LENGTH uint32 = 4096
	var max_key_len uint32 = KEY_LENGTH
	var max_value_len uint32 = VALUE_LENGTH
	var additional_nodes_per_block uint32 = 0 // you always get one node, this is how many ADDITIONAL nodes you want.
	return KEY_LENGTH, VALUE_LENGTH, max_key_len, max_value_len, additional_nodes_per_block
}

func test_4k(log *tools.Nixomosetools_logger, store stree_v_interfaces.Stree_v_backing_store_interface) {

	// var KEY_LENGTH uint32 = 20
	// var VALUE_LENGTH uint32 = 4096
	// var max_key_len uint32 = KEY_LENGTH
	// var max_value_len uint32 = VALUE_LENGTH
	// var additional_nodes_per_block uint32 = 0 // you always get one node, this is how many ADDITIONAL nodes you want.
	var KEY_LENGTH, VALUE_LENGTH, max_key_len, max_value_len, additional_nodes_per_block = get_init_params()

	var sample_key = Key_from_block_num(0)

	var ret, block_size = stree_v_lib.Calculate_block_size(log, "key", []byte("value"), max_key_len, max_value_len, additional_nodes_per_block)

	var stree_v *stree_v_lib.Stree_v = stree_v_lib.New_Stree_v(log, store, max_key_len,
		max_value_len, additional_nodes_per_block, block_size, sample_key, []byte("sample_value"))

	ret = stree_v.Startup(false)
	if ret != nil {
		tools.Error(log, "Unable to create stree: ", ret.Get_errmsg())
		return
	}

	var lib stree_v_test_lib = New_stree_v_test_lib(log)
	lib.Stree_4k_tests(stree_v, KEY_LENGTH, VALUE_LENGTH)
	stree_v.Shutdown()

}

func test_basics() {

	var loggertest *tools.Nixomosetools_logger = tools.New_Nixomosetools_logger(tools.DEBUG)

	loggertest.Set_level(tools.INFO)

	loggertest.Debug("hi from stu")
	loggertest.Info("hi from stu", " a second logger param", tools.Inttostring(749))
	loggertest.Error("hi from stu")

	loggertest.Set_level(tools.DEBUG)

	// you can also use the file store, but the problems tend to be in stree_v not in the physical storage.
	var mstore *stree_v_lib.Memory_store = stree_v_lib.New_memory_store(loggertest)

	var max_key_len uint32 = 20
	var max_value_len uint32 = 40
	var additional_nodes_per_block uint32 = 0 // you always get one node, this is how many ADDITIONAL nodes you want.

	var ret, block_size = stree_v_lib.Calculate_block_size(loggertest, "key", []byte("value"), max_key_len, max_value_len, additional_nodes_per_block)

	var stree_v *stree_v_lib.Stree_v = stree_v_lib.New_Stree_v(loggertest, mstore, max_key_len,
		max_value_len, additional_nodes_per_block, block_size, "sameple_key", []byte("sample_value"))

	ret = stree_v.Startup(false)
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
	var mstore *stree_v_lib.Memory_store = stree_v_lib.New_memory_store(log)

	// this is the key they offspring nodes will get by default.

	var key_type string = ("--")
	var value_type []byte = []byte("****")

	var ret, block_size = stree_v_lib.Calculate_block_size(log, key_type, value_type, KEY_LENGTH, VALUE_LENGTH, additional_nodes_per_block)

	var stree_v *stree_v_lib.Stree_v = stree_v_lib.New_Stree_v(log, mstore, KEY_LENGTH,
		VALUE_LENGTH, additional_nodes_per_block, block_size, key_type, value_type)

	ret = stree_v.Startup(false)
	if ret != nil {
		tools.Error(log, "Unable to create block storage: ", ret.Get_errmsg())
		return
	}

	var lib stree_v_test_lib = New_stree_v_test_lib(log)
	lib.Stree_test_offspring(stree_v, KEY_LENGTH, VALUE_LENGTH, additional_nodes_per_block)
	stree_v.Shutdown()
}

func test_tree_alone() {

	var log *tools.Nixomosetools_logger = tools.New_Nixomosetools_logger(tools.DEBUG)

	var KEY_LENGTH uint32 = 2
	var VALUE_LENGTH uint32 = 4 // this is how much data you can store in one node

	var additional_nodes_per_block uint32 = 0 // you always get one node, this is how many ADDITIONAL nodes you want.
	additional_nodes_per_block = 4            // this is how many nodes there are in one block referred to by a single key minus one, because you always get one.
	var mstore *stree_v_lib.Memory_store = stree_v_lib.New_memory_store(log)

	var key_type string = ("defkey")
	var value_type []byte = []byte("defvalue")

	var ret, block_size = stree_v_lib.Calculate_block_size(log, key_type, value_type, KEY_LENGTH, VALUE_LENGTH, additional_nodes_per_block)

	var stree_v *stree_v_lib.Stree_v = stree_v_lib.New_Stree_v(log, mstore, KEY_LENGTH,
		VALUE_LENGTH, additional_nodes_per_block, block_size, key_type, value_type)

	ret = stree_v.Startup(false)
	if ret != nil {
		tools.Error(log, "Unable to create block storage: ", ret.Get_errmsg())
		return
	}

	var lib stree_v_test_lib = New_stree_v_test_lib(log)

	lib.Stree_test_run(stree_v, KEY_LENGTH, VALUE_LENGTH)
	stree_v.Shutdown()
}
