// SPDX-License-Identifier: LGPL-2.1
// Copyright (C) 2021-2022 stu mark

// Package slookup_i_lib ... has a comment
package slookup_i_lib

import "github.com/nixomose/nixomosegotools/tools"

type Slookup_i_backing_store_interface interface {

	/* 6/3/2022 the stree interface is tailored to the stree implementation a bit
	   so we can't use it exactly for slookup, so we copied most of it here
		 and we'll just have to have a different implementation and caller
		 than we did for stree.
		 too bad. it would have been cool if the slookup matched stree enough
		 that we could reuse the interface and then it would have just worked. */
	/* the backing aligned file store and memory store I think will work fine
	   with the tlog. so there is that at least.
		 The direct access to the backing store should not even allow read or write
		 it should always go through the tlog. direct access to the tlog is for
		 initializing the header blocks and tlog and data, and for other
		 get-size kinda things. All actual reads and writes of data including headers
		 must go through tlog. */

	/* One of the sucky things we must do is write zeroes over the entry lookup table
	for init. For a file it doesn't matter, but if we are backed by a block device, and
	there's junk there, we may misinterpret junk as some kind of valid data and
	screw everything up. so at least if we're a block device, we must write zeroes over
	everything. Unless of course calling fallocate hole punch on a block device
	actually calls discard. bwahahahah I don't think so. */

	/* 7/22/2022 the zosbd2 layer calls slookup_i which calls the tlog to call this
	to do the actual writing of blocks to the backing store. */

	Init() tools.Ret

	Is_backing_store_uninitialized() (tools.Ret, bool)

	Startup(force bool) tools.Ret

	Shutdown() tools.Ret

	// remind me what this does again?
	// Load_block_header(len uint32) (tools.Ret, *[]byte)

	Load_block_data(block_num uint32) (tools.Ret, *[]byte)

	// and this? what's this for?
	// Store_block_header(n *[]byte) tools.Ret

	Store_block_data(block_num uint32, n *[]byte) tools.Ret

	//  this is owned by slookup_i, not backing store. Get_free_position() (ret tools.Ret, resp uint32)

	Get_total_blocks() (tools.Ret, uint32) // total blocks that can fit in this backing storage

	// there is no allocate and deallocate, there is only read and write, and the backing store either works or it doesn't.
	// Allocate(amount uint32) (tools.Ret, []uint32)

	// Deallocate() tools.Ret

	Wipe() tools.Ret // zero out the first block so as to make it inittable again

	Dispose() tools.Ret
}
