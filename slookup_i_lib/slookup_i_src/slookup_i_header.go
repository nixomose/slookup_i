// SPDX-License-Identifier: LGPL-2.1
// Copyright (C) 2021-2022 stu mark

/* this is the header for block 0, the superblock as it were. */

// package name must match directory name
package slookup_i_src

import (
	"bytes"
	"encoding/binary"

	"github.com/nixomose/nixomosegotools/tools"
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
	M_data_block_start_block      uint32 //  what block number the data blocks start at, this can move if we resize the transaction log
	M_free_position               uint32 // location of first free block, starts life at data_block_start_block
	// these are specific to the backing store.
	// M_alignment                   uint32 // size of block alignment, 0 = not aligned.
	// M_dirty                       uint32 // was this filestore shutdown cleanly.

	// slookup_i header format <xxxz update this>
	/*        magic                    store size in bytes
	00000000  5a 45 4e 53 54 35 4b 41 | 00 00 01 3d 4a 15 99 99  |ZENST5KA...=J...|
	          nodes/block|block size  | block count|root node
	00000010  00 00 01 01 00 00 14 38 | 0f b1 5d 42 00 00 00 01  |.......8..]B....|
	          free pos    alignment   | dirty
	00000020  00 00 00 01 00 00 00 00 | 00 00 00 00 00 00 00 00  |................|
	*/
}

func (this *Slookup_i_header) Serialize(log *tools.Nixomosetools_logger) (tools.Ret, *[]byte) {
	/* serialize this header into a byte array, which just goes to block zero, really  */

	var bb *bytes.Buffer = bytes.NewBuffer(make([]byte, 0))
	var err error = binary.Write(bb, binary.BigEndian, this) // this works because there's nothing but actual data fields.
	if err != nil {
		return tools.Error(log, "unable to serialize slookup header: ", err), nil
	}

	var bret []byte = bb.Bytes()
	return nil, &bret
}

func (this *Slookup_i_header) Deserialize(log *tools.Nixomosetools_logger, bs *[]byte) tools.Ret {
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
