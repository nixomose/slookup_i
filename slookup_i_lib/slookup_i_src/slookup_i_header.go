// SPDX-License-Identifier: LGPL-2.1
// Copyright (C) 2021-2022 stu mark

/* this is the header for block 0, the superblock as it were. */

// package name must match directory name
package slookup_i_src

type Slookup_i_header struct {
	// must be capitalized or we can deserialize because it's not exported...
	M_magic                       uint64
	M_data_block_size             uint32 // the number of bytes we need to store a block of data. this is the smallest unit the backing store can read/write
	M_total_blocks                uint32 // the total size of file/backing store we have to work with.
	M_block_group_count           uint32 // how many data blocks in a block group
	M_lookup_table_start_block    uint32 // what block number the lookup table starts at
	M_transaction_log_start_block uint32 // what block number the transaction log starts at
	M_data_block_start_block      uint32 //  what block number the data blocks start at, this can move if we resize the transaction log
	M_free_position               uint32 // location of first free block, starts life at data_block_start_block
	M_alignment                   uint32 // size of block alignment, 0 = not aligned.
	M_dirty                       uint32 // was this filestore shutdown cleanly.

	// slookup_i header format
	/*        magic                    store size in bytes
	00000000  5a 45 4e 53 54 35 4b 41 | 00 00 01 3d 4a 15 99 99  |ZENST5KA...=J...|
	          nodes/block|block size  | block count|root node
	00000010  00 00 01 01 00 00 14 38 | 0f b1 5d 42 00 00 00 01  |.......8..]B....|
	          free pos    alignment   | dirty
	00000020  00 00 00 01 00 00 00 00 | 00 00 00 00 00 00 00 00  |................|
	*/
}
