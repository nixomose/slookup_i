// SPDX-License-Identifier: LGPL-2.1
// Copyright (C) 2021-2022 stu mark

/* this is the header for block 0, the superblock as it were.
   this is block zero from slookup_i's point of view, in real life, if we're storing in a file
	 for example, the backing store will allocate the physical block zero for itself, and the physical
	 block 1 is where slookup will store its header. */

// Package slookup_i_src name must match directory name
package slookup_i_src

import (
	"bytes"
	"crypto"
	"crypto/md5"
	"encoding/binary"
	"encoding/json"
	"fmt"

	"github.com/nixomose/nixomosegotools/tools"
	slookup_i_lib_entry "github.com/nixomose/slookup_i/slookup_i_lib/slookup_i_entry"
	slookup_i_lib_interfaces "github.com/nixomose/slookup_i/slookup_i_lib/slookup_i_interfaces"
)

const ZENDEMIC_OBJECT_STORE_SLOOKUP_I_MAGIC uint64 = 0x5a454e4f53534c31 // ZENOSSL1  zendemic object store slookup I

const SLOOKUP_ERROR_INVALID_HEADER int = 1000

type Slookup_i_header struct {
	// must be capitalized or we can deserialize because it's not exported...
	M_magic                       uint64
	M_data_block_size             uint32 // the number of bytes we need to store a block of data. this is the smallest unit the backing store can read/write, and the maximum length of the value in 1 storable block, multiple blocks make up a storable unit (a block group)
	M_lookup_table_entry_count    uint32 // this is the number of blocks that are storable. multiplied by data_block_size is the total allocatable storage in bytes
	M_lookup_table_entry_size     uint32 // this is the number of bytes that a lookup table entry takes up
	M_total_backing_store_blocks  uint32 // the total size of file/backing store we have to work with, not the size of the block device we're supporting. it is okay to have less or more total blocks than lookup_table_entry_count. as in you can overprovision.
	M_block_group_count           uint32 // how many data blocks in a block group, also how many elements in the block_group_list a.k.a. m_offspring_per_node
	M_lookup_table_start_block    uint32 // what block number the lookup table starts at
	M_transaction_log_start_block uint32 // what block number the transaction log starts at
	M_data_block_start_block      uint32 // what block number the data blocks start at, this can move if we resize the transaction log
	M_free_position               uint32 // location of first free block, starts life at data_block_start_block

	/* these are specific to the backing store. it keeps track of alignment, and we rely on the backing store to tell us if
	   it was shutdown cleanly or not. arguably we don't need to know if we were shut down cleanly or not, we can find
		 out if we need to from the backing store, but basically when we start up, we run the transaction log, if there
		 was nothing in there, then it doesn't really matter if we were shut down cleanly or not. */
	// M_alignment                   uint32 // size of block alignment, 0 = not aligned.
	// M_dirty                       uint32 // was this filestore shutdown cleanly.

	// slookup_i header format <xxxz update this>
	/*        magic                    store size in bytes
	00000000  5a 45 4e 53 54 35 4b 41 | 00 00 01 3d 4a 15 99 99  |ZENST5KA...=J...|
	          nodes/block|block size  | block count|root node
	00000010  00 00 01 01 00 00 14 38 | 0f b1 5d 42 00 00 00 01  |.......8..]B....|
	          free pos                |
	00000020  00 00 00 01 00 00 00 00 | 00 00 00 00 00 00 00 00  |................|
	*/
}

func (this *Slookup_i_header) Dump(log *tools.Nixomosetools_logger) tools.Ret {

	var m map[string]string = make(map[string]string)

	magic := make([]byte, 8)
	binary.BigEndian.PutUint64(magic, uint64(this.M_magic))
	// not quite what I wanted. m["0001_magic"] = hexdump.Dump(magic)
	m["0001_magic"] = tools.Dump([]byte(magic))

	m["0002_data_block_size"] = tools.Prettylargenumber_uint64(uint64(this.M_data_block_size)) + " 0x" + fmt.Sprintf("%016x", this.M_data_block_size)
	m["0003_lookup_table_entry_count"] = tools.Prettylargenumber_uint64(uint64(this.M_lookup_table_entry_count)) + " 0x" + fmt.Sprintf("%016x", this.M_lookup_table_entry_count)

	m["0004_M_lookup_table_entry_size"] = tools.Prettylargenumber_uint64(uint64(this.M_lookup_table_entry_size)) + " 0x" + fmt.Sprintf("%016x", this.M_lookup_table_entry_size)
	m["0005_total_backing_store_blocks"] = tools.Prettylargenumber_uint64(uint64(this.M_total_backing_store_blocks)) + " 0x" + fmt.Sprintf("%016x", this.M_total_backing_store_blocks)
	m["0006_block_group_count"] = tools.Prettylargenumber_uint64(uint64(this.M_block_group_count)) + " 0x" + fmt.Sprintf("%016x", this.M_block_group_count)
	m["0007_lookup_table_start_block"] = tools.Prettylargenumber_uint64(uint64(this.M_lookup_table_start_block)) + " 0x" + fmt.Sprintf("%016x", this.M_lookup_table_start_block)
	m["0008_transaction_log_start_block"] = tools.Prettylargenumber_uint64(uint64(this.M_transaction_log_start_block)) + " 0x" + fmt.Sprintf("%016x", this.M_transaction_log_start_block)
	m["0009_data_block_start_block"] = tools.Prettylargenumber_uint64(uint64(this.M_data_block_start_block)) + " 0x" + fmt.Sprintf("%016x", this.M_data_block_start_block)
	m["0010_free_position"] = tools.Prettylargenumber_uint64(uint64(this.M_free_position)) + " 0x" + fmt.Sprintf("%016x", this.M_free_position)

	bytesout, err := json.MarshalIndent(m, "", " ")
	if err != nil {
		return tools.Error(log, "unable to marshal root node information into json: ", err)
	}

	var json string = string(bytesout)
	fmt.Println(json)
	return nil
}

func (this *Slookup_i_header) serialize(log *tools.Nixomosetools_logger) (tools.Ret, *[]byte) {
	/* serialize this header into a byte array, which just goes to block zero, really  */

	var bb *bytes.Buffer = bytes.NewBuffer(make([]byte, 0))
	var err error = binary.Write(bb, binary.BigEndian, this) // this works because there's nothing but actual data fields.
	if err != nil {
		return tools.Error(log, "unable to serialize slookup header: ", err), nil
	}

	var bret []byte = bb.Bytes()
	return nil, &bret
}

func (this *Slookup_i_header) serialized_size() uint32 {
	return 8 + // M_magic
		4 + // M_data_block_size
		4 + // M_lookup_table_entry_count
		4 + // M_lookup_table_entry_size
		4 + // M_total_blocks
		4 + // M_block_group_count
		4 + // M_lookup_table_start_block
		4 + // M_transaction_log_start_block
		4 + // M_data_block_start_block
		4 // M_free_position
}

func (this *Slookup_i_header) deserialize(log *tools.Nixomosetools_logger, bs *[]byte) tools.Ret {
	/* deserialize incoming data into this object's fields.
	interesting to note that an unused header (one that has previously not ever been written to) might
	be all zeroes, or it might be all junk. But that would yield a bad magic, so we should catch
	that early. */

	var bb *bytes.Buffer = bytes.NewBuffer(*bs)

	var err error = binary.Read(bb, binary.BigEndian, this)
	if err != nil {
		return tools.Error(log, "unable to deserialize slookup header: ", err)
	}

	return nil
}

func (this *Slookup_i_header) Store_header(log *tools.Nixomosetools_logger,
	tlog slookup_i_lib_interfaces.Transaction_log_interface, initting bool) tools.Ret {
	/* everybody goes through here so we can calculate checksum */
	// do a checksum, hash whatever of the data in the header and add it to the end.

	var data *[]byte
	var ret tools.Ret
	ret, data = this.serialize(log)
	if ret != nil {
		return ret
	}
	var m5 = md5.Sum(*data)
	var hashed_data = append(*data, m5[:]...)

	if initting {
		/* so the very first time we do this, we have to write out CHECK_START_BLANK_BYTES
		so that the second time we come in, we can read a whole header check bytes block without
		getting EOF */
		var to_write = tools.Maxint(CHECK_START_BLANK_BYTES, int(this.M_data_block_size))
		var pad_len = to_write - len(hashed_data)
		for pad_len > 0 { // slow and crappy but we only ever do it once.
			hashed_data = append(hashed_data, 0)
			pad_len = to_write - len(hashed_data)
		}
	}
	return tlog.Write_block(0, &hashed_data)
}

func (this *Slookup_i_header) Initial_load_and_verify_header(
	log *tools.Nixomosetools_logger,
	tlog slookup_i_lib_interfaces.Transaction_log_interface,
	check_device_params bool,
	m_verify_slookup_i_addressable_blocks uint32,
	m_verify_slookup_i_block_group_count uint32,
	m_verify_slookup_i_data_block_size uint32,
	m_verify_slookup_i_entry_max_value_length uint32, // data_block_size * block_group_count
	m_verify_slookup_i_total_backing_store_blocks uint32) tools.Ret {
	/* read the first block and see if it's got our magic number, and validate size and blocks and all that. */
	/* for storage status, the values passed in device are bunk, so skip the checks
	   (this check_device_params) because they will fail. */

	/* now that we can read from the backing store, get the header and verify that the data_block_size in the header
	   that defines the backing store layout, matches what the caller sent.
		 this is only ever called once on startup so it reads from the backing
		 store directly. the header spends its life in memory and is just written
		 to disk as part of transactions. */

	var bytes_read uint32
	var data *[]byte
	var ret tools.Ret
	ret, data = tlog.Read_block(0)
	if ret != nil {
		return ret
	}

	// pull off the hash at the end before we do anything else
	if len(*data) < crypto.MD5.Size() {
		return tools.Error(log, "unable to read slookup header, not enough data for checkssum, length is only ", len(*data),
			" it must be at least ", crypto.MD5.Size)
	}
	var header_data = (*data)[0:int(this.serialized_size())]
	var m5 = (*data)[int(this.serialized_size()) : this.serialized_size()+uint32(crypto.MD5.Size())]

	var m5check = md5.Sum(header_data)
	if bytes.Equal(m5check[:], m5) == false {

		return tools.ErrorWithCodeNoLog(log, SLOOKUP_ERROR_INVALID_HEADER,
			"unable to read slookup_i header, hash check failed")
	}

	*data = header_data
	if len(*data) < int(this.serialized_size()) {
		return tools.ErrorWithCodeNoLog(log, SLOOKUP_ERROR_INVALID_HEADER,
			"unable to read slookup header of ", this.serialized_size(), " got back ", bytes_read)
	}

	ret = this.deserialize(log, data)
	if ret != nil {
		return ret
	}

	/* this means the header doesn't match what we expect, and we should init the backing storage,
	I can see where this could be a dangerously bad idea, so we're just going to error out, let
	the user deal with it. */
	if this.M_magic != ZENDEMIC_OBJECT_STORE_SLOOKUP_I_MAGIC {
		return tools.ErrorWithCodeNoLog(log, SLOOKUP_ERROR_INVALID_HEADER,
			"magic number doesn't match in slookup_i header")
	}

	if check_device_params {
		// see if they header matches what the caller specified, not all header fields are worth checking

		if this.M_lookup_table_entry_count != m_verify_slookup_i_addressable_blocks {
			return tools.Error(log, "the recorded lookup entry count ", this.M_lookup_table_entry_count,
				" doesn't equal the supplied addressable_block count of ", m_verify_slookup_i_addressable_blocks)
		}

		if this.M_data_block_size != m_verify_slookup_i_data_block_size {
			return tools.Error(log, "the stored data block size ", this.M_data_block_size, " doesn't equal ",
				"the supplied block size of ", m_verify_slookup_i_data_block_size)
		}

		if this.M_block_group_count != m_verify_slookup_i_block_group_count {
			return tools.Error(log, "the stored block group count ", this.M_block_group_count, " doesn't equal ",
				"the supplied block group count of ", m_verify_slookup_i_block_group_count)
		}

		if this.M_total_backing_store_blocks != m_verify_slookup_i_total_backing_store_blocks {
			return tools.Error(log, "the stored total backing store blocks ", this.M_total_backing_store_blocks, " doesn't equal ",
				"the supplied total backing store blocks of ", m_verify_slookup_i_total_backing_store_blocks)
		}

		if this.M_data_block_size*this.M_block_group_count != m_verify_slookup_i_entry_max_value_length {
			return tools.Error(log, "the stored max value length ", this.M_data_block_size*this.M_block_group_count, " doesn't equal ",
				"the supplied max value length of ", m_verify_slookup_i_entry_max_value_length)
		}

		var measure_entry *slookup_i_lib_entry.Slookup_i_entry = slookup_i_lib_entry.New_slookup_entry(log, 0,
			m_verify_slookup_i_entry_max_value_length, m_verify_slookup_i_block_group_count)

		var measure_entry_serialized_size uint32 = measure_entry.Serialized_size()

		if this.M_lookup_table_entry_size != measure_entry_serialized_size {
			return tools.Error(log, "the stored entry serialized size ", this.M_lookup_table_entry_size, " doesn't equal ",
				"the calculated entry serialized size of ", measure_entry_serialized_size)
		}

		// we could maybe also verify the positions of the start of lookup table, tlog and data blocks, but those could change and that's valid.

	} else { // if we should check device fields against the header or are we just reading the header to display.
		/* there are a few places that use the user/catalog supplied initial block size (for initial creation)
		   and therefore is wrong if just getting storage status, so we set it to what the on-disk header says. */
		// this.m_initial_block_size = this.m_header.M_block_size
		// this.m_initial_block_count = this.m_header.M_block_count
		// this.m_initial_file_store_block_alignment = this.m_header.M_alignment
	}
	return nil // all is well.
}
