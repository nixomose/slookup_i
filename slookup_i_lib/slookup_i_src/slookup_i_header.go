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

	"github.com/nixomose/nixomosegotools/tools"
	slookup_i_lib_entry "github.com/nixomose/slookup_i/slookup_i_lib/slookup_i_entry"
)

type Slookup_i_header struct {
	// must be capitalized or we can deserialize because it's not exported...
	M_magic                       uint64
	M_data_block_size             uint32 // the number of bytes we need to store a block of data. this is the smallest unit the backing store can read/write, and the maximum length of the value in 1 storable block, multiple blocks make up a storable unit (a block group)
	M_lookup_table_entry_count    uint32 // this is the number of blocks that are storable. multiplied by data_block_size is the total allocatable storage in bytes
	M_lookup_table_entry_size     uint32 // this is the number of bytes that a lookup table entry takes up
	M_total_blocks                uint32 // the total size of file/backing store we have to work with, not the size of the block device we're supporting. it is okay to have less or more total blocks than lookup_table_entry_count. as in you can overprovision.
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

func (this *Slookup_i_header) Store_header(initting bool) tools.Ret {
	/* everybody goes through here so we can calculate checksum */
	// do a checksum, hash whatever of the data in the header and add it to the end.

	var data *[]byte
	var ret tools.Ret
	ret, data = this.Serialize()(this.log)
	if ret != nil {
		return ret
	}
	var m5 = md5.Sum(*data)
	var hashed_data = append(*data, m5[:]...)

	if initting {
		/* so the very first time we do this, we have to write out CHECK_START_BLANK_BYTES
		so that the second time we come in, we can read a whole header check bytes block without
		getting EOF */
		var to_write = tools.Maxint(CHECK_START_BLANK_BYTES, int(this.m_initial_block_size))
		var pad_len = to_write - len(hashed_data)
		for pad_len > 0 { // slow and crappy but we only ever do it once.
			hashed_data = append(hashed_data, 0)
			pad_len = to_write - len(hashed_data)
		}
	}
	return this.write_raw_data(0, &hashed_data)
}

func (this *Slookup_i_header) load_header_and_check_magic(check_device_params bool) Ret {
	/* read the first block and see if it's got our magic number, and validate size and blocks and all that. */
	/* for storage status, the values passed in device are bunk, so skip the checks
	   (this check_device_params) because they will fail. */

	var bytes_read uint32
	var data []byte
	var ret tools.Ret
	ret, data = this.Read_raw_data(0)
	if ret != nil {
		return ret
	}

	// pull off the hash at the end before we do anything else
	if len(data) < crypto.MD5.Size() {
		return Error(this.log, "unable to read header, not enough data for checkssum, length is only ", crypto.MD5.Size)
	}
	var header_data = data[0:int(this.m_header.Serialized_size())]
	var m5 = data[int(this.m_header.Serialized_size()) : this.m_header.Serialized_size()+uint32(crypto.MD5.Size())]

	var m5check = md5.Sum(header_data)
	if bytes.Compare(m5check[:], m5) != 0 {
		return Error(this.log, "unable to read header, hash check failed")
	}

	data = header_data
	if len(data) < int(this.m_header.Serialized_size()) {
		return Error(this.log, "unable to read header of ", this.m_header.Serialized_size(), " got back ", bytes_read)
	}

	ret = this.m_header.deserialize(this.log, &data)
	if ret != nil {
		return ret
	}

	/* this means the header doesn't match what we expect, and we should init the backing storage,
	I can see where this could be a dangerously bad idea, so we're just going to error out, let
	the user deal with it. */
	if this.m_header.M_magic != ZENDEMIC_OBJECT_STORE_SLOOKUP_I_MAGIC {
		return Error(this.log, "magic number doesn't match in backing storage")
	}

	if check_device_params {
		// see if they header matches what the caller specified
		if this.m_header.M_block_size != this.m_initial_block_size {
			return Error(this.log, "block size store cached in backing storage ", this.m_header.M_block_size,
				" doesn't match initial block size ", this.m_initial_block_size)
		}

		if this.m_header.M_block_count != this.m_initial_block_count {
			return Error(this.log, "block count cached in backing storage ", this.m_header.M_block_count,
				" doesn't match initial block count ", this.m_initial_block_count)
		}

		if this.m_header.M_alignment != this.m_initial_file_store_block_alignment {
			return Error(this.log, "alignment ", this.m_header.M_alignment,
				" doesn't match initial alignment ", this.m_initial_file_store_block_alignment)
		}
	} else { // if we should check device fields against the header or are we just reading the header to display.
		/* there are a few places that use the user/catalog supplied initial block size (for initial creation)
		   and therefore is wrong if just getting storage status, so we set it to what the on-disk header says. */
		this.m_initial_block_size = this.m_header.M_block_size
		this.m_initial_block_count = this.m_header.M_block_count
		this.m_initial_file_store_block_alignment = this.m_header.M_alignment
	}
	return nil // all is well.
}

func (this *Slookup_i_header) Initial_load_and_verify_header() tools.Ret {
	/* now that we can read from the backing store, get the header and verify that the data_block_size in the header
	   that defines the backing store layout, matches what the caller sent.
		 this is only ever called once on startup so it reads from the backing
		 store directly. the header spends its life in memory and is just written
		 to disk as part of transactions. */
	var data *[]byte
	var ret tools.Ret
	// this is about the only thing that goes after the backing store directly and doesn't go through the transaction log
	if ret, data = this.m_storage.Load_block_data(0); ret != nil {
		return ret
	}

	var measure_entry *slookup_i_lib_entry.Slookup_i_entry = slookup_i_lib_entry.New_slookup_entry(this.log, 0,
		this.m_verify_slookup_i_data_block_size, this.m_verify_slookup_i_block_group_count)

	var measure_entry_serialized_size uint32 = measure_entry.Serialized_size()
	this.m_entry_size_cache = measure_entry_serialized_size

	if ret = this.m_header.Deserialize(this.log, data); ret != nil {
		return ret
	}

	// now just compare the fields we can

	if this.m_header.M_lookup_table_entry_count != this.m_verify_slookup_i_addressable_blocks {
		return tools.Error(this.log, "the recorded lookup entry count ", this.m_header.M_lookup_table_entry_count,
			" doesn't equal the supplied addressable_block count of ", this.m_verify_slookup_i_addressable_blocks)
	}

	if this.m_header.M_data_block_size != this.m_verify_slookup_i_data_block_size {
		return tools.Error(this.log, "the stored data block size ", this.m_header.M_data_block_size, " doesn't equal ",
			"the supplied block size of ", this.m_verify_slookup_i_data_block_size)
	}

	if this.m_header.M_block_group_count != this.m_verify_slookup_i_block_group_count {
		return tools.Error(this.log, "the stored block group count ", this.m_header.M_block_group_count, " doesn't equal ",
			"the supplied block group count of ", this.m_verify_slookup_i_block_group_count)
	}

	if this.m_header.M_lookup_table_entry_size != measure_entry_serialized_size {
		return tools.Error(this.log, "the stored entry serialized size ", this.m_header.M_lookup_table_entry_size, " doesn't equal ",
			"the calculated entry serialized size of ", measure_entry_serialized_size)
	}

	// we could maybe also verify the positions of the start of lookup table, tlog and data blocks, but those could change and that's valid.
	return nil
}

func (this *Slookup_i_header) store_header() tools.Ret {
	/* write the header to disk */

	var data *[]byte
	var ret tools.Ret

	if ret, data = this.m_header.Serialize(this.log); ret != nil {
		return ret
	}

	/* writes to the header go through the tlog so that header changes can also be
	part of a transaction. */
	if ret = this.Write(0, data); ret != nil {
		return ret
	}

	return nil
}
