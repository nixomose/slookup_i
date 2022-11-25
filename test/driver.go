// SPDX-License-Identifier: LGPL-2.1
// Copyright (C) 2021-2022 stu mark

package main

import (
	"os"

	"github.com/nixomose/nixomosegotools/tools"
	slookup_i_lib "github.com/nixomose/slookup_i/slookup_i_lib/slookup_i_interfaces"
	"github.com/nixomose/slookup_i/slookup_i_lib/slookup_i_src"
)

func make_file_store_aligned(log *tools.Nixomosetools_logger, storage_file string, device_directio bool, device_alignment uint32,
	PHYSICAL_BLOCK_SIZE uint32, slookup_i_data_block_size uint32, block_count uint32) (tools.Ret, *slookup_i_src.File_store_aligned) {
	/* block_count here is the number of data_block_size blocks we can store */

	var alignment = device_alignment // PHYSICAL_BLOCK_SIZE // 4k will use 8k per block because of our stree block header pushes the whole node size to a bit over 4k

	// if directio is set alignment must be % PHYSICAL_BLOCK_SIZE == 0, or reads and writes will fail.
	if device_directio {
		if alignment == 0 {
			alignment = PHYSICAL_BLOCK_SIZE
			device_alignment = alignment // force caller to get this update
		}

		if alignment%PHYSICAL_BLOCK_SIZE != 0 {
			return tools.Error(log, "your alignment must fall on a ", PHYSICAL_BLOCK_SIZE, " boundary if directio is on. ",
				"alignment: ", alignment, " % ", PHYSICAL_BLOCK_SIZE, " is ", alignment%PHYSICAL_BLOCK_SIZE), nil
		}
	} else {
		if alignment == 0 {
			alignment = slookup_i_data_block_size
			device_alignment = alignment // same thing here, if they didn't specify alignment, we tell the device what it should be
		}
	}

	/* first we have to see if we're doing directio or default io path so we can inject that into the
	   filestore aligned object */
	var iopath slookup_i_src.File_store_io_path
	if device_directio {
		iopath = slookup_i_src.New_file_store_io_path_directio()
	} else {
		iopath = slookup_i_src.New_file_store_io_path_default()
	}

	/* so the backing physical store for the stree is the block device or file passed... */

	var fstore *slookup_i_src.File_store_aligned = slookup_i_src.New_File_store_aligned(log,
		storage_file, slookup_i_data_block_size, block_count, alignment, iopath)

	return nil, fstore
}

func bring_up(filename string) (tools.Ret, *slookup_i_src.Slookup_i) {

	var log *tools.Nixomosetools_logger = tools.New_Nixomosetools_logger(tools.DEBUG)

	var data_block_size, block_group_count, addressable_blocks, total_blocks = get_init_params()

	var ret tools.Ret
	var directio bool = false // if this is true it must be a block device not a file, because iopath will not create the file for directio
	var device_alignment uint32 = 4096
	var physical_block_size uint32 = 4096

	var fstore *slookup_i_src.File_store_aligned
	ret, fstore = make_file_store_aligned(log, filename, directio, device_alignment, physical_block_size, data_block_size, total_blocks)
	if ret != nil {
		return ret, nil
	}
	var tlog = slookup_i_src.New_Tlog(log, fstore, data_block_size, total_blocks)

	var slookup *slookup_i_src.Slookup_i = slookup_i_src.New_Slookup_i(log, fstore, tlog,
		addressable_blocks, block_group_count, data_block_size, total_blocks)

	return slookup.Startup(false), slookup
}

func bring_down(slookup *slookup_i_src.Slookup_i) tools.Ret {

	ret := slookup.Shutdown()
	if ret != nil {
		return ret
	}
	return nil
}

func make_block_data(val byte, data_block_size uint32) *[]byte {
	// make a block of data
	var value_type = make([]byte, 0)
	var dot = make([]byte, 1)

	dot[0] = val
	var n uint32
	for n < data_block_size {
		value_type = append(value_type[:], dot...)
		val++
		dot[0] = val
		n++
	}
	return &value_type
}

func test_two_blocks(filename string) tools.Ret {
	ret, slookup := bring_up(filename)
	if ret != nil {
		return ret
	}

	/* write block 0 write block 1 erase block 0 */
	var data_block_size, _, _, _ = get_init_params()

	var data *[]byte
	data = make_block_data(0x21, data_block_size) // A

	if ret = slookup.Write(42, data); ret != nil {
		return ret
	}

	data = make_block_data(0x22, data_block_size) // B
	if ret = slookup.Write(43, data); ret != nil {
		return ret
	}

	slookup.Diag_dump_slookup_header()
	slookup.Diag_dump_block(42)
	slookup.Diag_dump_block(43)
	slookup.Diag_dump_block(28) // where the reverse lookup entries are.

	if ret = slookup.Discard(42); ret != nil {
		return ret
	}

	/* now write a 2-block_group_count entry */
	data = make_block_data(0x23, data_block_size*2) // C
	if ret = slookup.Write(44, data); ret != nil {
		return ret
	}

	if ret = bring_down(slookup); ret != nil {
		return ret
	}
	return nil
}

func main() {

	//test_basics()

	var log *tools.Nixomosetools_logger = tools.New_Nixomosetools_logger(tools.DEBUG)

	/* total data blocks *should* be block_group_count * user_addressable_blocks plus the number of blocks we need
	for the header, the lookup table and the journal and all the padding, but we allow for the flexibility
	of underprovisioning if you want so you can specify that we actually can hold less data than the number of
	blocks demands we should be able to. And if we compress well, it is quite possible you still won't run out
	even if you underprovision. */

	/* we could conceivably calculate this by adding up all the space for the header, padding,
	lookup table, padding, tlog, padding, and actual data_block storage. but the idea is that you can
	under provision, and you can just tell us how much space you have and hope you don't run out. */
	/* of course if you don't provide enough room for the lookup table and the tlog and at least one actual data_block storage
	block, well, it's not going to work. */

	var data_block_size, block_group_count, user_addressable_blocks, total_backing_store_blocks = get_init_params()

	log.Debug("data_block_size: ", data_block_size)
	log.Debug("block_group_count: ", block_group_count)
	log.Debug("user_addressable blocks: ", user_addressable_blocks)
	log.Debug("total_backing_store_blocks: ", total_backing_store_blocks)

	// value_type := make_block_data(0x21, data_block_size)

	var testfile = "/tmp/fstore"
	os.Remove(testfile)

	{ // init the filestore header make it ready to go

		/* so the backing physical store for the slookup is the block device or file passed... */
		// var fstore *slookup_i_src.File_store_aligned = slookup_i_src.New_File_store_aligned(log, testfile,
		// 	data_block_size, addressable_blocks, alignment, iopath)

		var ret tools.Ret
		var directio bool = false // if this is true it must be a block device not a file, because iopath will not create the file for directio
		var device_alignment uint32 = 4096
		var physical_block_size uint32 = 4096

		var fstore *slookup_i_src.File_store_aligned
		ret, fstore = make_file_store_aligned(log, testfile, directio, device_alignment,
			physical_block_size, data_block_size, total_backing_store_blocks)
		if ret != nil {
			return
		}
		var tlog = slookup_i_src.New_Tlog(log, fstore, data_block_size, total_backing_store_blocks)

		var slookup *slookup_i_src.Slookup_i = slookup_i_src.New_Slookup_i(log, fstore, tlog,
			user_addressable_blocks, block_group_count, data_block_size, total_backing_store_blocks)

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

	test_two_blocks(testfile)

	// {

	// 	var fstore *slookup_i_src.File_store_aligned = slookup_i_src.New_File_store_aligned(log, testfile,
	// 		data_block_size, addressable_blocks, alignment, iopath)

	// 	test_4k(log, fstore, &iopath, alignment)

	// }

	// {
	// 	// test everything again with a memory store.
	// 	var ret tools.Ret
	// 	var mstore *slookup_i_src.Memory_store = slookup_i_src.New_memory_store(log)
	// 	mstore.Init()
	// 	var tlog = slookup_i_src.New_Tlog(log, mstore, data_block_size, total_blocks)
	// 	if ret = tlog.Startup(false); ret != nil {
	// 		return
	// 	}

	// 	test_4k(log, mstore, &iopath, alignment)
	// }

}

func get_init_params() (data_block_size uint32, block_group_count uint32, user_addressable_blocks uint32, total_backing_store_blocks uint32) {

	/* data_block_size * block_group_count is the number of bytes in a user facing block, that can be
	for example compressed down to a minimum of 1 data_block size */
	data_block_size = 4096 // bytes in a data block, the fundamental unit of storage in slookup_i
	block_group_count = 5  // a.k.a. additional_nodes_per_block, how many data_blocks are stored in a user-facing 'block'

	user_addressable_blocks = 1000 // the number of addressable blocks in this slookup_i table, defined by the user.
	/* a data block is 5k because a block from the user's point of view is stored in an entry. there are
	   5 data blocks in that user block (so we can maybe compress it down to 1 actually stored).
		 so if there block_Group_count is 5, and the user says there are 1000 blocks then the total data storable
		 is 5 * 1000 * 4096 bytes */

	/* the total amount of storable bytes is data_block_size * block_group_count * lookup_table_entry_count(addressable blocks) */

	/* we could conceivably calculate this by adding up all the space for the header, padding,
	lookup table, padding, tlog, padding, and actual data_block storage. but the idea is that you can
	under provision, and you can just tell us how much space you have and hope you don't run out. */
	/* of course if you don't provide enough room for the lookup table and the tlog and at least one actual data_block storage
	block, well, it's not going to work. */

	total_backing_store_blocks = 500 // for testing let's go with this.

	return
}

func test_4k(log *tools.Nixomosetools_logger, fmstore slookup_i_lib.Slookup_i_backing_store_interface,
	iopath *slookup_i_src.File_store_io_path, alignment uint32) tools.Ret {
	var ret = test_4k_functions(log, fmstore, iopath, alignment)
	if ret != nil {
		return ret
	}

	return nil
}

func test_4k_functions(log *tools.Nixomosetools_logger, fmstore slookup_i_lib.Slookup_i_backing_store_interface,
	iopath *slookup_i_src.File_store_io_path, alignment uint32) tools.Ret {

	var ret tools.Ret
	var data_block_size uint32
	var block_group_count uint32
	var addressable_blocks uint32
	var total_blocks uint32
	data_block_size, block_group_count, addressable_blocks, total_blocks = get_init_params()

	var tlog = slookup_i_src.New_Tlog(log, fmstore, data_block_size, total_blocks)
	if ret = tlog.Startup(false); ret != nil {
		return ret
	}

	var slookup_i *slookup_i_src.Slookup_i = slookup_i_src.New_Slookup_i(log, fmstore, tlog, addressable_blocks,
		block_group_count, data_block_size, total_blocks)

	ret = slookup_i.Startup(false)
	if ret != nil {
		tools.Error(log, "Unable to create slookup: ", ret.Get_errmsg())
		return ret
	}

	var lib slookup_i_test_lib = New_slookup_i_test_lib(log)
	if ret = lib.Slookup_4k_tests(slookup_i); ret != nil {
		return ret
	}
	if ret = lib.Slookup_test_writing_zero(slookup_i); ret != nil {
		return ret
	}

	slookup_i.Shutdown()
	return nil
}

func test_basics() tools.Ret {

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
	var initted bool
	if ret, initted = slookup.Is_initialized(); ret != nil {
		return ret
	}
	if initted == false {
		slookup.Init()
	}
	ret = slookup.Startup(false)
	if ret != nil {
		/* so this may fail on a brand new setup where the header hasn't been written yet.
		   if we are told this is an init startup, then write the new header and try again. */
		if ret.Get_errcode() != slookup_i_src.SLOOKUP_ERROR_INVALID_HEADER {
			return tools.Error(loggertest, "Unable to create block storage: ", ret.Get_errmsg())
		}
		if ret = slookup.Init(); ret != nil {
			return ret
		}
		// now try again it should work
		ret = slookup.Startup(false)
		if ret != nil {
			tools.Error(loggertest, "Unable to start up block storage after init: ", ret.Get_errmsg())
			return ret
		}
	}
	ret = slookup.Shutdown()
	if ret != nil {
		return tools.Error(loggertest, "Unable to shut down block storage: ", ret.Get_errmsg())
	}
	return nil
}
