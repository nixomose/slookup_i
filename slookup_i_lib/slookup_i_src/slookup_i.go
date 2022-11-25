// SPDX-License-Identifier: LGPL-2.1
// Copyright (C) 2021-2022 stu mark

/* this is the implementation of slookup_i.
   it is half modeled on stree, with the mother and offspring nodes but
	 instead of being a tree, it's just going to be a simple lookup table.
	 stree had everything in one node. here we have a lookup table entry with all the metadata
	 (kept as small as possible since we have to preallocate the whole thing)
	 and the data is stored starting with the first block after however many blocks
	 it takes to store the entire lookup table.

	 the lookup table entry has the block number where the data is stored and information
	 about the offspring nodes if any or who the mother node is, if any.
	 this is all way simpler than stree, you just look up the location you want, and
	 voila, data. 2 reads max.
	 The way we're going to add complexity is by implementing the tlog as part of all
	 reads and writes.
	 we never did implement the transaction log in stree_v. oh well.
	 doesn't matter this will be faster anyway. */

/* since we're not a tree with nodes that have mothers and offspring, the naming convention I'm going
with for now, is a data_block is a block 4k or whateever of data, the smallest unit we can store.
a block_group is all the data_blocks that comprise a set of mother+offspring data blocks.
There is also a reverse lookup table buried in the main lookup table, so that it is possible to find
the lookup table entry that contains a given data block position. */

/* 9/3/2022 I spent a long time thinking about if slookup_i should even have direct access to the backing store.
all reads and writes go through tlog, but there are a few manage-the-backing-store that could be added to tlog
that just passes through to the backing store, but it's one of those things where we're making a worse design just
so we can have a pretty interface and things like wipe, discard, and mark_end would exist only as passthrough
functions in tlog, which breaks the function-that-does-nothing-but-call-a-function rule for the sake of not having
slookup_i control both the tlog and the backing store.
So I say we do it the right way: do what needs to be done that makes sense, not what is pretty.
therefore slookup inits the tlog and inits the backing store separately. tlog is not responsible for initting
the backing store. that's not its job, it's a transaction log, not a backing store manager. */

// Package slookup_i_src name must match directory name
package slookup_i_src

import (
	"context"
	"crypto/md5"
	"fmt"
	"sync"
	"syscall"

	"github.com/nixomose/nixomosegotools/tools"
	slookup_i_lib_entry "github.com/nixomose/slookup_i/slookup_i_lib/slookup_i_entry"
	slookup_i_lib_interfaces "github.com/nixomose/slookup_i/slookup_i_lib/slookup_i_interfaces"
	"golang.org/x/sync/errgroup"
)

// this is the block that the lookup table starts at, block 0 is the header.

const LOOKUP_TABLE_START_BLOCK uint32 = 10             // make some room in case we need it some day.
const TRANSACTION_LOG_START_PADDING_BLOCKS uint32 = 10 // more room, this is the number of blocks after the lookup table before the transaction log starts
const DATA_BLOCKS_START_PADDING_BLOCKS uint32 = 10     // what a horrible name for this., this is the number of blocks after the transaction log, before the data blocks start.

type Slookup_i struct {

	/* this is the guts of the lookup table. it'll be a bit more refined since we worked out all the details in
	   stree and this is simpler than the stree.
		 so you have a header for the file in block zero, blocks 1-n are the lookup table entries (slookup_i_entry).
		 the number of blocks taken up is size of block device / size of slookup_i_entry +1 (unless it aligns perfectly).
		 Then we leave space for the transaction log. It can be stored in the file, or externally, but if it's in the file
		 it's after the lookup table before the data starts. The reason is that if we need to resize the backing store,
		 we can flush the tlog, then move data blocks from the beginning to end to grow, or from the end to beginning to shrink,
		 make a new tlog at the end of the new ending location of the lookup table, and move the datablocks to fill in the gaps.
		 we'll probably never get around to implementing that, but it's at least workable this way.
		 after the transaction log is where the data blocks start, aligned and sized exactly to the data unlike stree.
		 the other cute thing we can do is if we need to expand or shrink the block device, we can move the n+1 block
		 to the end and shuffle the mother and offspring nodes as appropriate, the opposite of deleting.
		 so we need a generic block mover than can move a block from anywhere to anywhere else.
		 same thing with making it smaller, we can shrink the block device and therefore the lookup table
		 and just move the free_space-1 block to the n-1 position and so on. */

	log *tools.Nixomosetools_logger

	m_header Slookup_i_header

	m_storage                 slookup_i_lib_interfaces.Slookup_i_backing_store_interface // direct access to the backing store for init and setup
	m_transaction_log_storage slookup_i_lib_interfaces.Transaction_log_interface         // the backing store mechanism for writing stree_v data

	m_entry_size_cache uint32 // this is the (cached) serialized size of the entry given the number of blocks in a block group

	/* 12/26/2020 only one of anything in the interface can happen at once, so here's the lock for it. */
	interface_lock sync.Mutex

	m_verify_slookup_i_addressable_blocks         uint32 // this is only used to make sure the client knows what they're doing.
	m_verify_slookup_i_block_group_count          uint32 // same here
	m_verify_slookup_i_data_block_size            uint32 // same here
	m_verify_slookup_i_total_backing_store_blocks uint32 // same here

	debugprint bool
}

func New_Slookup_i(l *tools.Nixomosetools_logger,
	b slookup_i_lib_interfaces.Slookup_i_backing_store_interface,
	t slookup_i_lib_interfaces.Transaction_log_interface, /* entry_size uint32, this gets calculated */
	addressable_blocks uint32, block_group_count uint32, data_block_size uint32, total_backing_store_blocks uint32) *Slookup_i {
	/* so addressable_blocks * block_group_count is the total number of blocks we can store.
		 block_group_count * data_block_size is the size of one storable block that can be compressed (for example)
		 down to data_block size. so that represents how many bytes are storable in a block from the caller's point
		 of view.
	   addressable_blocks * block_group_count * data_block_size is the number of addressable bytes.
		 and total_blocks is how many physical blocks actually exist in the backing store, which should be
		 available in the backing_store_interface. */

	var s Slookup_i
	s.log = l

	// we just need to get the serialized size of the lookup entry so we can space out the header correctly. */
	var size_calc *slookup_i_lib_entry.Slookup_i_entry = slookup_i_lib_entry.New_slookup_entry(l, 0, 0, block_group_count)
	s.m_entry_size_cache = size_calc.Serialized_size()

	s.init_header(data_block_size, addressable_blocks, s.m_entry_size_cache, total_backing_store_blocks, block_group_count)
	/* storage gives you direct access to the backing store so you can init and such */
	s.m_storage = b
	/* the transcation log gives you transactional reads and writes to that backing storage. */
	s.m_transaction_log_storage = t
	/* block_group_count * data_block_size  is the max data size we can store per write insert request.
	 * This is what the client will see as the "block size" which is the max data we can store in a client 'block'. */

	// we get these from the caller for creation or to verify that it matches what's already in the backing store if it exists.
	s.m_verify_slookup_i_addressable_blocks = addressable_blocks
	s.m_verify_slookup_i_block_group_count = block_group_count
	s.m_verify_slookup_i_data_block_size = data_block_size
	s.m_verify_slookup_i_total_backing_store_blocks = total_backing_store_blocks
	return &s
}

func (this *Slookup_i) init_header(data_block_size uint32, lookup_table_entry_count uint32,
	lookup_table_entry_size uint32, total_backing_store_blocks uint32, block_group_count uint32) {

	this.m_header.M_magic = ZENDEMIC_OBJECT_STORE_SLOOKUP_I_MAGIC
	this.m_header.M_data_block_size = data_block_size
	this.m_header.M_lookup_table_entry_count = lookup_table_entry_count
	this.m_header.M_lookup_table_entry_size = lookup_table_entry_size
	this.m_header.M_total_backing_store_blocks = total_backing_store_blocks
	this.m_header.M_block_group_count = block_group_count

	this.m_header.M_lookup_table_start_block = this.Get_first_lookup_table_start_block()
	this.m_header.M_transaction_log_start_block = this.Get_first_transaction_log_start_block()
	this.m_header.M_data_block_start_block = this.Get_first_data_block_start_block()
	this.m_header.M_free_position = this.m_header.M_data_block_start_block

	// // dirty maybe is a backing store thing? alignment too
	// slookup will have it's own dirty flag. someday.
	// h.M_alignment = alignment
	// h.M_dirty = 0
}

func (this *Slookup_i) Get_logger() *tools.Nixomosetools_logger {
	return this.log
}

func (this *Slookup_i) Is_initialized() (tools.Ret, bool) {
	/* check the first 4k for zeroes. */

	var ret, uninitted = this.m_storage.Is_backing_store_uninitialized()
	if ret != nil {
		return ret, false
	}

	if uninitted {
		return nil, false
	}
	return nil, true
}

func (this *Slookup_i) Init() tools.Ret {
	/* init the backing store, as in if it's a filestore, write the header info
	so it becomes initted, if it's a block device, zero out the lookup table. */
	var ret = this.m_storage.Init() // this should write the file header if it's a disk/file/block device
	if ret != nil {
		return ret
	}

	if ret = this.m_transaction_log_storage.Init(); ret != nil {
		return ret
	}

	if ret = this.zero_out_lookup_table(); ret != nil {
		return ret
	}
	// and finally write out the slookup_i header to the first block

	return this.write_slookup_header()
}

func (this *Slookup_i) write_slookup_header() tools.Ret {
	/* use the tlog to write the slookup header update to block 0 */

	var data *[]byte
	var ret tools.Ret
	if ret, data = this.m_header.serialize(this.log); ret != nil {
		return ret
	}
	var m5 = md5.Sum(*data)
	var hashed_data = append(*data, m5[:]...)

	// if initting {
	// 	/* so the very first time we do this, we have to write out CHECK_START_BLANK_BYTES
	// 	so that the second time we come in, we can read a whole header check bytes block without
	// 	getting EOF */
	// 	var to_write = tools.Maxint(CHECK_START_BLANK_BYTES, int(this.m_initial_block_size))
	// 	var pad_len = to_write - len(hashed_data)
	// 	for pad_len > 0 { // slow and crappy but we only ever do it once.
	// 		hashed_data = append(hashed_data, 0)
	// 		pad_len = to_write - len(hashed_data)
	// 	}
	// }

	return this.m_transaction_log_storage.Write_block(0, &hashed_data)
}

func (this *Slookup_i) zero_out_lookup_table() tools.Ret {
	/* unlike stree where we allocate everything as we need it, the slookup on disk format
	   requires that the lookup table be in a state where we can tell the difference between
	   blocks already written to and blocks never written to.
	   filesystems do this by trimming everything when they first write their filesystem metadata
	   and basically that's what we have to do here too.
	   the backing store might be able to trim/discard for us, but if not, we're going to have
	   to write a lot of zeroes.
	   the assumption is that an entry can deserialize correctly from a pile of zeros.
	*/

	var ret tools.Ret = this.discard_lookup_table()

	if ret != nil {
		if ret.Get_errcode() != int(syscall.ENAVAIL) {
			return ret
		}
		// the discard was unavaiable, do it the hard way
		var start_block = this.Get_first_lookup_table_start_block()
		var end_block = start_block + this.Get_lookup_table_storage_block_count()
		if ret = this.m_transaction_log_storage.Write_block_range(start_block, end_block, nil); ret != nil {
			return ret
		}
	}
	return nil
}

func (this *Slookup_i) discard_lookup_table() tools.Ret {
	/* get the blocks assigned to the lookup table, and call discard on them */
	var start_block uint32 = this.Get_first_lookup_table_start_block()
	var end_block uint32 = start_block + this.Get_lookup_table_storage_block_count()
	//	var ctx context.Context

	var group *errgroup.Group
	group, _ = errgroup.WithContext(context.Background())
	var lp uint32
	for lp = start_block; lp < end_block; lp++ {
		var block_num uint32 = lp // make a copy of lp to pass to go routine
		group.Go(func() error {
			var ret tools.Ret
			if ret = this.m_storage.Discard_block(block_num); ret != nil {
				return ret
			}
			return nil
		})
	}
	var err = group.Wait()
	if err != nil {
		return tools.Error(this.log, "unable to discard lookup table blocks ", start_block, " to ", end_block, ". err: ", err)
	}
	return nil
}

func (this *Slookup_i) Startup(force bool) tools.Ret {
	/* start everything up and verify that the lookup entry size and the data block size match what's defined in the backing store */

	// need to start up the transaction log here too?
	var ret = this.m_storage.Startup(force)
	if ret != nil {
		return ret
	}
	if ret = this.m_transaction_log_storage.Startup(force); ret != nil {
		return ret
	}

	if ret = this.m_header.Initial_load_and_verify_header(this.log,
		this.m_transaction_log_storage, true,
		this.m_verify_slookup_i_addressable_blocks,
		this.m_verify_slookup_i_block_group_count,
		this.m_verify_slookup_i_data_block_size,
		this.m_verify_slookup_i_total_backing_store_blocks); ret != nil {
		return ret
	}
	return nil
}

func (this *Slookup_i) Shutdown() tools.Ret {
	/* make sure it's all on disk, note while this might flush the current data to
	disk if it wasn't already, what it will not do is complete a transaction if it is
	in flight. If we are for some reason shutting down cleanly enough to be running but
	not cleanly enough that the application was able to complete an atomic transaction
	we don't want to write the transcation end into the log, so when it gets replayed
	it will not apply the last (incomplete) transaction. */
	var ret = this.m_transaction_log_storage.Shutdown()
	if ret != nil {
		return nil // xxxz?
	}
	return this.m_storage.Shutdown()
}

func (this *Slookup_i) Print(log *tools.Nixomosetools_logger) {
	/* Not sure what we're doing here, but probably dumping all lookup entries. */
	var ret, free_position = this.Get_free_position()
	if ret != nil {
		fmt.Println(ret.Get_errmsg())
		return
	}
	var first_data_position uint32 = this.Get_first_data_block_start_block()

	// the number of allocated blocks
	var allocated_blocks uint32 = free_position - first_data_position
	var lp uint32
	for lp = first_data_position; lp < free_position; lp++ {
		var ret, e = this.Lookup_entry_load(lp)
		if ret != nil {
			fmt.Println(ret.Get_errmsg())
			return
		}
		//var _ = n //
		fmt.Print("[" + tools.Uint32tostring(lp) + "] " + e.Dump(false))
	}
	fmt.Println()
	fmt.Println("first data postion: ", first_data_position, " free position: ", free_position)
	fmt.Println("free position: ", free_position)
	fmt.Println("allocated blocks: ", allocated_blocks)
}

/* * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * */

/* The counts.
   There are 4 main storage areas that take up the blocks in the backing store:
	 1) the header (1 block at block 0)
	 2) the lookup table (however many blocks it takes to store an entry for every block in the data blocks section)
	 3) the transaction log (haven't decided yet)
	 4) the data blocks themselves (the rest of the blocks available)
	 so here we provide the three functions that work out how many blocks each section
	 takes up, and then there's three functions to work out the starting block
	 for each section given the size of each section and padding in between them.
	 Keep in mind you can overprovision, so just because you allocate space for 1000
	 lookup table entries, that doesn't mean you need 1000 data blocks afterwards.
	 Having more is wasteful/useless because they will never get used, having less
	 is what compression is all about. */

func (this *Slookup_i) Get_block_group_size_in_bytes() uint32 {
	/* this returns the number of bytes of user storable data in a lookup entry, it is not the size of the data block.
	 	 * as in, it is the data_block_size * block_group_count, not just data_block_size.
		 * this is used to report to the user how much space is available to store, so it should be used in the
		 * used/total block count * this number to denote the number of actual storable bytes. */

	return this.m_header.M_data_block_size * this.m_header.M_block_group_count
}

func (this *Slookup_i) Get_data_block_size_in_bytes() uint32 {
	/* this returns the number of bytes in one data block. */

	return this.m_header.M_data_block_size
}

func (this *Slookup_i) Get_lookup_entry_size_in_bytes() uint32 {
	/* return the size of the slookup_i entry (as it would be serialized on disk) in bytes without the value. */
	// this never changes why are we putting a lock around it
	return this.m_entry_size_cache
}

func (this *Slookup_i) Get_total_blocks_count() uint32 {
	/* return the total number of blocks in the backing store.
	   this isn't the number of usable data storage blocks, it's the entire thing, including the header block 0,
		 the lookup table, the tlog, the data blocks, everything. */

	return this.m_header.M_total_backing_store_blocks
}

func (this *Slookup_i) Get_lookup_table_entry_count() uint32 {
	/* return the total number of entries in the lookup table, which is the
	size provided by the user when they created this slookup_i definition. */
	//this never changes why lock?
	return this.m_header.M_lookup_table_entry_count
}

func (this *Slookup_i) Get_lookup_table_storage_block_count() uint32 {
	/* The serialized lookup table entries are laid back to back, and take up however many
	blocks they take up.
	so basically this function returns just the number of blocks that the entire lookup table takes to store,
	including the partially filled one at the end. */

	var entry_size uint32 = this.Get_lookup_entry_size_in_bytes()
	var lookup_table_entry_count uint32 = this.Get_lookup_table_entry_count()
	var data_block_size uint32 = this.Get_data_block_size_in_bytes()
	if data_block_size == 0 {
		this.log.Error("sanity failure; slookup_i data block size is zero.")
		return 0
	}
	var bytes_in_lookup_table uint64 = uint64(lookup_table_entry_count) * uint64(entry_size)
	var blocks_in_lookup_table uint64 = bytes_in_lookup_table / uint64(data_block_size)
	if bytes_in_lookup_table%uint64(data_block_size) != 0 {
		blocks_in_lookup_table += 1 // round up for the leftover unfilled last lookup table block
	}
	// we should cache this at some point, except I think we only call this once on startup to figure out the on disk layout.
	return uint32(blocks_in_lookup_table)
}

/* the idea of using the transaction log for everything includes the header block because updating things like
the free position will become part of a transaction. as such since we hit it a lot, we're going to cache
it here so we don't actually read the block every single time we ask for it. it is sorta a storage thing
but it is also sorta a slookup_i thing, but it's more a slookup_i thing, so we'll put it here. */

func (this *Slookup_i) Get_transaction_log_block_count() uint32 {
	/* I have no idea at the moment. */
	return 100
}

func (this *Slookup_i) Get_storable_data_blocks_count() (tools.Ret, uint32) {
	/* This is the size of usable storage in blocks for storing data. It's the remainder of the
	     backing storage after you subtract the space needed for the lookup table and the transaction log
			 and the padding, and the header.
			 Remember, the lookup table is a linear map for the size of the block device that the caller specified
			 (not the total number of available blocks)
			 the data block storage is just a dumping ground in any order for the data pointed to by the lookup table.
			 which means, I had it all wrong, the lookup table doesn't refer to the positional data in the backing store
			 it refers to the position in the block device that is then used to look up where the data is in
			 the data block area, which means you can easily over provision which is fine, but that also means
			 we can't calculate the size of the lookup table, we have to be told how big the block device we're making
			 a map for is.
			 And that's the point. The user must specify both the amount of space the block device is defined to be able to
			 store, AND the actual amount of storage (in data blocks) that you want to use to store that data, which can
			 be less than how much you say you can store, because... compression.

		   So this is the number of total actual blocks in the backing store, minus the size of the
		   lookup table, minus the size of the transaction log, and all the padding between them, because the
			 user doesn't tell actually tell us how many data blocks they want to be able to store, they just give us
			 a total_blocks and we work out the split here. */

	var ret tools.Ret
	var total_blocks uint32
	if ret, total_blocks = this.m_storage.Get_total_blocks(); ret != nil {
		return ret, 0
	}
	var transaction_log_block_count uint32 = this.Get_transaction_log_block_count()
	var lookup_table_storage_block_count uint32 = this.Get_lookup_table_storage_block_count()

	var total_blocks_available_for_storage = total_blocks - LOOKUP_TABLE_START_BLOCK // this skips the header and padding

	total_blocks_available_for_storage -= lookup_table_storage_block_count
	total_blocks_available_for_storage -= TRANSACTION_LOG_START_PADDING_BLOCKS
	total_blocks_available_for_storage -= transaction_log_block_count
	total_blocks_available_for_storage -= DATA_BLOCKS_START_PADDING_BLOCKS

	/* and after all that, is how many blocks are left available to actually store data. */
	return nil, total_blocks_available_for_storage
}

/* In fact this is one of the mistakes I made in stree, the backing block store shouldn't have any understanding
of what goes in those blocks, having the filestore know what offspring are was a mistake. the positional layout of
the lookup table, the log and the data blocks is entirely a slookup_i thing, so here is where we actually figure out
what the locations of the three sections are, in terms of their block locations. */

func (this *Slookup_i) Get_first_lookup_table_start_block() uint32 {
	return LOOKUP_TABLE_START_BLOCK
}

func (this *Slookup_i) Get_first_transaction_log_start_block() uint32 {
	/* There are/can be three things in the file. the lookup table always comes first.
	   then there's a pile of blocks for the transaction log (or zero if it is not stored there, you wouldn't want that a network away)
		 and then the actual data blocks. this returns the absolute block position of the first block
		 holding transaction log information, after the end of the lookup table (and padding). */
	// we can make a cache later if need be, but I think this only happens once.

	var lookup_table_storage_block_count uint32 = this.Get_lookup_table_storage_block_count()
	var first_transaction_log_block uint32 = this.Get_first_lookup_table_start_block() +
		lookup_table_storage_block_count +
		TRANSACTION_LOG_START_PADDING_BLOCKS
	return first_transaction_log_block
}

func (this *Slookup_i) Get_first_data_block_start_block() uint32 {
	/* this is the start of the transaction log block position plus the size of the transaction log plus some padding. */

	var transaction_log_start_block uint32 = this.Get_first_transaction_log_start_block()
	var transaction_log_block_count uint32 = this.Get_transaction_log_block_count()

	var data_blocks_start_block uint32 = transaction_log_start_block + transaction_log_block_count +
		DATA_BLOCKS_START_PADDING_BLOCKS
	return data_blocks_start_block
}

func (this *Slookup_i) Get_block_group_count() uint32 {
	/* this just returns the value in the header, the number of entries in the block_group array */

	return this.m_header.M_block_group_count
}

func (this *Slookup_i) Get_free_position() (tools.Ret, uint32) {
	/* for now, read the header block from the transaction log, deserialize it and return the free position */
	// we can make a cache later.
	// I guess the header is the cache.
	var pos = this.m_header.M_free_position
	// maybe some validation check here xxxz
	return nil, pos
}

func (this *Slookup_i) Set_free_position(new_free_position uint32) tools.Ret {
	var ret tools.Ret
	if ret = this.check_data_block_limits(new_free_position); ret != nil {
		return ret
	}
	this.m_header.M_free_position = new_free_position
	return nil
}

/* * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * */
// the limits checkers

func (this *Slookup_i) check_lookup_table_limits(block_num uint32) tools.Ret {
	/* So there are three check block limits functions.
	   1) this one, the simplest, just make sure that block_num is a valid block
	      given the number of blocks that can be stored in this block device.
	 			As in, if there are 10 blocks (and n blocks in the block group), we make sure block_num is >= 0 and < 10
	 	 2) the second one is a bit less obvious, when we're reading or writing actual
	      lookup table blocks when we update a lookup table entry, we have to make sure
	 			that the block we're updating exists within the bounds of the blocks that comprise
	 			the lookup table.
		 3) the third one is just to check that the data block being checked exists within
	      the range of blocks (absolute from the start of the entire set of data storage blocks).
	 			so if there are 10 data blocks starting at block 3, make sure block_num >= 3  and < 13.
				data block references are absolute from the start of the backing block storage
	 	 4) I guess we need a version of 1 and 2 for the transaction log too... */

	// this is function #1, check the block num for the number of blocks the user specified as the size of the block device,

	var lookup_table_entry_count uint32 = this.Get_lookup_table_entry_count()
	if block_num > lookup_table_entry_count {
		return tools.Error(this.log, "sanity failure, somebody is trying to operate on a block entry beyond the end of the lookup table: ",
			"block_num: ", block_num, " total blocks: ", lookup_table_entry_count)
	}
	return nil
}

func (this *Slookup_i) check_lookup_table_entry_block_limits(block_num uint32) tools.Ret {
	/* for functions that operate on the lookup table part of the data store, verify that
	block_num fits within the bounds of the blocks we have allocated for this block device
	for the lookup table itself. */

	// this is function #2

	// first figure out how many blocks are allocated for the lookup table.

	var start_block = this.Get_first_lookup_table_start_block()
	var count = this.Get_lookup_table_entry_count()
	var lookup_table_end_block = start_block + count

	if block_num < start_block {
		return tools.Error(this.log, "sanity failure, somebody is trying to operate on a lookup table entry block before the start ",
			"of the lookup table: block_num: ", block_num, " lookup table start block: ", start_block)
	}
	if block_num > lookup_table_end_block {
		return tools.Error(this.log, "sanity failure, somebody is trying to operate on a lookup table entry block past the end ",
			"of the lookup table: block_num: ", block_num, " lookup table end block: ", lookup_table_end_block)
	}
	return nil
}

func (this *Slookup_i) check_data_block_limits(data_block_num uint32) tools.Ret {
	/* for functions that operate on the data block portion of the data store, verify that
	data_block_num fits within the bounds of the absolution position of blocks we have allocated for data
	for this block device.
	keep in mind, that if there are 10 blocks and there are 5 offspring/block_group_count, that doesn't matter,
	the specified size of the block device has nothing to do with how much space we have to actually store data
	blocks, and that's what matters here. the allowed space is the data block starting position up to total_blocks. */

	// this is function #3

	if data_block_num == 0 {
		return tools.Error(this.log, "sanity failure, somebody is trying to operate on a data_block at block zero.")
	}
	var first_data_block_position = this.Get_first_data_block_start_block()
	if data_block_num < first_data_block_position {
		return tools.Error(this.log, "sanity failure, somebody is trying to operate on a data_block position: ", data_block_num,
			" which is before the first data block location: ", first_data_block_position)
	}

	var last_possible_data_block_position uint32 = this.Get_total_blocks()

	if data_block_num >= last_possible_data_block_position { // check for off by one here.
		return tools.Error(this.log, "sanity failure, somebody is trying to operate on a data_block position: ", data_block_num,
			" which is at or after the last possible data block location: ", last_possible_data_block_position)
	}
	return nil
}

/* * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * */

/* The stores
   and gets */

func (this *Slookup_i) internal_lookup_entry_blocks_load(block_num uint32) (ret tools.Ret, start_byte_pos uint32, start_block uint32,
	end_pos uint32, end_block uint32, start_offset uint32, alldata *[]byte) {

	/* figure out what block(s) this entry is in and load it/them, storage can only load one block at a time
	so we might have to concatenate. something we'll have to fix with goroutines someday.
	oh look, we did goroutines already.
	start block is the first block, end block is the number after the last block, as in end block is not inclusive.
	so we return all the stuff we calculated in case the caller is interested, and the alldata we return is
	all of the data from all of the blocks that made up the entry request. So if the entry lives in one block
	it returns the data for that block, if the entry spills over the boundary of two blocks, then it returns
	two blocks of data, and finally it returns the position of the entry within this buffer. */

	start_byte_pos = block_num * this.Get_lookup_entry_size_in_bytes()
	start_block = (start_byte_pos / this.Get_data_block_size_in_bytes()) + this.Get_first_lookup_table_start_block()

	end_pos = start_byte_pos + this.Get_lookup_entry_size_in_bytes()
	end_block = (end_pos / this.Get_data_block_size_in_bytes()) + this.Get_first_lookup_table_start_block()
	end_block += 1 // end_block is one higher than the one we want to read

	if ret = this.check_lookup_table_entry_block_limits(start_block); ret != nil {
		return
	}
	/* the end block is 1 + the start block so we can read a range, but to actually check the range
	   limits, the end block we're reading must be within the range of actual lookup table blocks
		 so we subtract one for this check */
	if ret = this.check_lookup_table_entry_block_limits(end_block - 1); ret != nil {
		return
	}
	if ret, alldata = this.m_transaction_log_storage.Read_block_range(start_block, end_block); ret != nil {
		return
	}

	/* get the position of this entry in this alldata. start pos is beginning of alldata, we just have to find the offset
	   which we can do by getting the remainder of the start pos to the block boundary whiche we read. */
	start_offset = start_byte_pos % (start_block * this.Get_data_block_size_in_bytes())
	return

	// this is all in tlog now.
	// var number_of_blocks = end_block - start_block + 1

	// var rets = make(chan tools.Ret)
	// var alldata_lock sync.Mutex

	// *alldata = make([]byte, number_of_blocks*this.Get_data_block_size_in_bytes())

	// for lp := start_block; lp < (end_block + 1); lp++ {
	// 	var destposstart = lp * this.Get_data_block_size_in_bytes()
	// 	var destposend = destposstart + this.Get_data_block_size_in_bytes()
	// 	go func(lpin uint32, destposstart uint32, destposend uint32) {
	// 		var data *[]byte
	// 		ret, data = this.m_transaction_log_storage.Read_block(lpin)
	// 		if ret == nil {
	// 			// copy the data into the correct place in the alldata array
	// 			alldata_lock.Lock()
	// 			var copied = copy((*alldata)[destposstart:destposend], *data)
	// 			if copied != len(*data) {
	// 				rets <- tools.Error(this.log, "didn't get all the data from an entry block read, expected: ", data, " got: ", copied)
	// 				alldata_lock.Unlock()
	// 				return
	// 			}
	// 			alldata_lock.Unlock()
	// 		}

	// 		rets <- ret
	// 	}(lp, destposstart, destposend)

	// 	var final_ret tools.Ret = nil
	// 	for wait := 0; wait < int(end_block-start_block+1); wait++ {
	// 		ret = <-rets
	// 		if ret != nil {
	// 			final_ret = ret
	// 		}
	// 	}
	// 	// alldata should be filled correctly if all went well
	// 	ret = final_ret
	// }

}

func (this *Slookup_i) Lookup_entry_load(block_num uint32) (tools.Ret, *slookup_i_lib_entry.Slookup_i_entry) {
	/* read only the lookup entry for a block num.
	this function can't read transaction log blocks, or data blocks. */
	// xxxz this should probably interface lock and have a lookup_entry_load_internal to not lock
	var ret = this.check_lookup_table_limits(block_num)
	if ret != nil {
		return ret, nil
	}

	var _, _, _, _, start_offset uint32
	var alldata *[]byte
	ret, _, _, _, _, start_offset, alldata = this.internal_lookup_entry_blocks_load(block_num)
	if ret != nil {
		return ret, nil
	}
	var entrydata = (*alldata)[start_offset : start_offset+this.Get_lookup_entry_size_in_bytes()]
	var entry = slookup_i_lib_entry.New_slookup_entry(this.log, block_num,
		this.Get_data_block_size_in_bytes(), this.Get_block_group_count())
	ret = entry.Deserialize(this.log, &entrydata)
	return ret, entry
}

func (this *Slookup_i) Lookup_entry_store(block_num uint32, entry *slookup_i_lib_entry.Slookup_i_entry) tools.Ret {
	/* Store this lookup entry at this block num position in the lookup table. this will require a read update
	write cycle of one or two blocks depending on if the entry straddles the border of two blocks.
	we run everything through the transaction logger because this is where it counts. */

	/* since we read update write we have to make sure we don't do this twice at one time
	   so we have to lock this entire event */
	this.interface_lock.Lock()
	defer this.interface_lock.Unlock()
	return this.lookup_entry_store_internal(block_num, entry)
}

func (this *Slookup_i) lookup_entry_store_internal(block_num uint32, entry *slookup_i_lib_entry.Slookup_i_entry) tools.Ret {
	/* internal version of above that doesn't lock the interface look for internal callers who have already locked it */

	var ret = this.check_lookup_table_limits(block_num)
	if ret != nil {
		return ret
	}

	var _, start_block, _, end_block, start_offset uint32
	var alldata *[]byte
	ret, _, start_block, _, end_block, start_offset, alldata = this.internal_lookup_entry_blocks_load(block_num)
	if ret != nil {
		return ret
	}
	// get the entry in byte form
	var entrydata *[]byte
	if ret, entrydata = entry.Serialize(); ret != nil {
		return ret
	}

	// and write it over the entry in the block where the entry lives.
	var end_offset = start_offset + this.Get_lookup_entry_size_in_bytes()
	var copied int = copy((*alldata)[int(start_offset):end_offset], (*entrydata)[:])
	if copied != int(this.Get_lookup_entry_size_in_bytes()) {
		return tools.Error(this.log, "lookup_entry_store failed to update the block while copying the entry data into it. ",
			"expected to copy: ", this.Get_lookup_entry_size_in_bytes(), " only copied ", copied)
	}

	/* now we have to write the block(s) back */
	ret = this.m_transaction_log_storage.Write_block_range(start_block, end_block, alldata)
	if ret != nil {
		return ret
	}

	return nil
}

func (this *Slookup_i) Data_block_load(entry *slookup_i_lib_entry.Slookup_i_entry) (tools.Ret, *[]byte) {
	/* given an entry, go get all of the members in the block group and lay them out in a byte array
	   and return it, similar to tlog.read_block_range except we're getting random blocks, because the blocks
		 in the block_group_list can be anywhere, even though the represent one contiguous block of data. */
	var ret tools.Ret
	var alldata *[]byte

	var block_list []uint32 = *entry.Get_block_group_list()
	var block_list_length = entry.Get_block_group_lengthxxxz()

	// this must know to only read up to the number of allocated blocks, not the entire array
	ret, alldata = this.m_transaction_log_storage.Read_block_list(block_list, block_list_length) // absolute block position
	if ret != nil {
		return ret, nil
	}
	return nil, alldata
}

func (this *Slookup_i) Data_block_store(entry *slookup_i_lib_entry.Slookup_i_entry) tools.Ret {
	/* write the actual data blocks from the value in this entry, into the blocks in the block_group array */
	var ret tools.Ret
	var alldata *[]byte = entry.Get_value()

	// var actual_count = entry.Get_block_group_length() // this is how many are allocated, not/<= block_group_count
	// the above is correct, we need to only write the block list of allocated blocks not all 5 with zeros.
	var block_list []uint32 = *entry.Get_block_group_list()
	var block_list_length = entry.Get_block_group_lengthxxxz()
	// same thing, only write allocated blocks not entire block_group_list array
	ret = this.m_transaction_log_storage.Write_block_list(block_list, block_list_length, alldata)
	if ret != nil {
		return ret
	}
	return nil
}

// not sure if we need this
/* I don't think we do, slookup deals with entries and an entry might have 5 blocks
in its group block list, but the data_block_load above takes care of reading the whole thing in.
it's somebody else's probably to decompress it, but we shouldn't worry about reading individual
parts of a group block list which is what this looks like it's trying to do. */

// func (this *Slookup_i) internal_data_block_load_list_position(entry *slookup_i_lib_entry.Slookup_i_entry,
// 	list_position uint32) (tools.Ret, *[]byte) {
// 	/* given a lookup table entry, and the array position in the block_group list,
// 	load the data the entry refers to and return it. we can't store it in entry.value, because we're not getting the
// 	whole value, we're only getting one block in the block_group. this just raw reads the data block from the correct location.
// 	All data block locations are relative to the beginning of the block device, not the
// 	start position of the data after the lookup table (actually after transaction log). The reason is we may resize the
// 	lookup table someday and we don't want the blocks to move when the start-of-data block
// 	moves. */

// 	if entry == nil {
// 		return tools.Error(this.log, "sanity failure, Lookup_entry_load got nil entry."), nil
// 	}

// 	/* not to be confused with the block_num the user refers to. this is the absolute position location of the data */
// 	var ret, data_block_num = entry.Get_data_block_lookup_pos(list_position)
// 	if ret != nil {
// 		return ret, nil
// 	}

// 	ret = this.check_data_block_limits(data_block_num)
// 	if ret != nil {
// 		return ret, nil
// 	}

// 	var data *[]byte
// 	ret, data = this.m_transaction_log_storage.Read_block(data_block_num) // absolute block position
// 	if ret != nil {
// 		return ret, nil
// 	}

// 	if uint32(len(*data)) > this.Get_data_block_size_in_bytes() {
// 		return tools.Error(this.log, "transaction_log read block for data block num: ", data_block_num,
// 			" returned data of length: ", len(*data),
// 			" which is more than the max data block size: ", this.Get_data_block_size_in_bytes()), nil
// 	}
// 	if uint32(len(*data)) < this.Get_data_block_size_in_bytes() {
// 		return tools.Error(this.log, "transaction_log read block for data block num: ", data_block_num,
// 			" returned data of length: ", len(*data),
// 			" which is more shorter than the max data block size: ", this.Get_data_block_size_in_bytes()), nil
// 	}

// 	/* I was confusing two things here, reading a single block will alway be data_block_size_in_bytes in
// 	length, we only return a list part, the caller is the one reading the entire block and must resize. */
// 	// /* a single data block is always this.m_max_value_length bytes long, the lookup table entry knows the length
// 	// of the data in the whole block group, so shorten the data we're returning if it's not the entire block */
// 	// var position_of_last_byte_in_this_block uint32 = list_position * this.m_data_block_size
// 	// var block_group_value_length = entry.Get_value_length()
// 	// if position_of_last_byte_in_this_block > block_group_value_length {
// 	// 	/* this is (must be) the last block_group_list in the value, and it doesn't fill out this block
// 	// 	so we must shorten the data in this block to the size of the value for the last block, which we are. */
// 	// 	if (position_of_last_byte_in_this_block - block_group_value_length) > this.m_data_block_size {
// 	// 		/* so this one's weird, this means that they asked for a block in the block_group list beyond
// 	// 		where any data could possibly be given the length of the block_group of data. */
// 	// 		return tools.Error(this.log, "sanity failure: data_block_load for list_position: ", list_position,
// 	// 			" of ", this.m_data_block_size, " bytes each, is more than a block longer than the value of this ",
// 	// 			"block_group which is: ", block_group_value_length, " bytes."), nil // hope that makes sense. should never happen. :-)
// 	// 	}
// 	// 	var length_of_this_data_block = block_group_value_length % this.m_data_block_size
// 	// 	*data = (*data)[0:length_of_this_data_block]
// 	// }

// 	return nil, data
// }

func (this *Slookup_i) calculate_block_group_count_for_value(value_length uint32) (tools.Ret, uint32) {
	/* return how many blocks in a block group we need for a value of this length */

	// unlike stree there is no mother node, so it is possible to have data of zero length taking up zero blocks.
	if value_length == 0 {
		var rval uint32 = 0
		return nil, rval
	}

	var group_size = this.Get_block_group_size_in_bytes()
	if value_length > group_size {
		return tools.Error(this.log,
			"value size ", value_length, " is too big to fit in ", (this.m_header.M_block_group_count), " blocks of ",
			this.m_header.M_data_block_size, " length totaling ", group_size), 0
	}

	var nnodes uint32 = value_length / this.m_header.M_data_block_size
	if value_length%this.m_header.M_data_block_size != 0 {
		nnodes++ // there was some more data that spilled over to the next node
	}
	return nil, nnodes
}

func (this *Slookup_i) reverse_lookup_entry_set(data_block uint32, entry_block_num uint32) tools.Ret {
	/* the idea here is that when we allocate a data block to store data in, we have to update the reverse
	   lookup table entry for that block so that if somebody needs to delete a block and pull this one, they know
	   what forward lookup entry has the block_group_list pos that points to the data_block that we just stored/allocated.

		 so here we are given a data block position (that was just allocated) and the entry block position that it was
	   allocated to (the addressaable block that was written to). we find, read, update and rewrite the entry block
		 that has the reverse lookup for this data_block
	   which means whoever calls this must have everything written to disk because we may pick up and rewrite any
	   entry. the reverse lookup can be anywhere. */

	// used as block_num to find the entry
	// we store block_group_count number of reverse lookup array entries in each entry.
	var reverse_lookup_entry_num uint32 = data_block / this.Get_block_group_count()
	// the position in the entry
	var reverse_lookup_entry_pos uint32 = data_block % this.Get_block_group_count()

	var ret tools.Ret
	var entry *slookup_i_lib_entry.Slookup_i_entry
	if ret, entry = this.Lookup_entry_load(reverse_lookup_entry_num); ret != nil {
		return ret
	}
	if ret = entry.Set_reverse_lookup_pos(reverse_lookup_entry_pos, entry_block_num); ret != nil {
		return ret
	}
	if ret = this.lookup_entry_store_internal(reverse_lookup_entry_num, entry); ret != nil {
		return ret
	}
	return nil
}

func (this *Slookup_i) reverse_lookup_entry_get(data_block uint32) (ret tools.Ret, entry *slookup_i_lib_entry.Slookup_i_entry,
	block_group_pos uint32) {
	/* opposite of above, given a data block, go get the entry that holds the reverse lookup information for
	that block, get the index, get the block num for the entry that refers to the data_block,
	read the entry in, and search that entry's block_group list for the block_group array position
	that has the requested data block to reverse lookup, return the block_num entry,
	the block_group array index and the value in it.
	actually don't return that last bit, duh, that's the value they passed us. */

	// used as block_num to find the entry
	// we store block_group_count number of reverse lookup array entries in each entry.
	var reverse_lookup_entry_num uint32 = data_block / this.Get_block_group_count()
	// the position in the entry
	var reverse_lookup_entry_pos uint32 = data_block % this.Get_block_group_count()

	var reverse_entry *slookup_i_lib_entry.Slookup_i_entry
	if ret, reverse_entry = this.Lookup_entry_load(reverse_lookup_entry_num); ret != nil {
		return ret, nil, 0
	}
	var block_num uint32
	if ret, block_num = reverse_entry.Get_reverse_lookup_pos(reverse_lookup_entry_pos); ret != nil {
		return ret, nil, 0
	}
	// now go get the forward entry for block_num
	if ret, entry = this.Lookup_entry_load(block_num); ret != nil {
		return ret, nil, 0
	}

	// now look through it's block_group list looking for data_block
	var rp uint32
	var block_group_list_length = entry.Get_block_group_lengthxxxz()

	for rp = 0; rp < block_group_list_length; rp++ {
		var block_group_pos_value uint32
		if ret, block_group_pos_value = entry.Get_block_group_pos(rp); ret != nil {
			return ret, nil, 0
		}
		if block_group_pos_value == data_block {
			return nil, entry, rp
		}
	}
	return tools.Error(this.log, "sanity failure: data_block: ", data_block, " not found while following reverse lookup entry: ",
		reverse_entry.Get_entry_pos(), " referring to forward lookup entry ", entry.Get_entry_pos()), nil, 0
}

// this is probably not used
// func (this *Slookup_i) update(block_num uint32, new_value *[]byte) tools.Ret {
// 	// update the data for this block with new value
// 	// return error if there was a read or write problem.

// 	/* 11/3/2020 update with offspring is not so simple. If we are storing more than what's there, we need
// 	 * to add blocks, if we are storing less, we need to delete. We can't leave leftover junk laying around,
// 	 * it will waste space, and the system can't account for it. You can't have a data block with no data in it.
// 	 * it will mess up the mother->offspring list when you try and delete it. */
// 	/* for slookup_i, for any blocks we are adding or removing (or moving for any reason) we need to update
// 	the lookup table entry for the block_group list entry, and the reverse lookup table entry. */

// 	if new_value == nil {
// 		return tools.Error(this.log, "trying to update with null value")
// 	}

// 	var ret = this.check_lookup_table_limits(block_num)
// 	if ret != nil {
// 		return ret
// 	}

// 	var entry *slookup_i_lib_entry.Slookup_i_entry
// 	ret, entry = this.Lookup_entry_load(block_num) // this is the lookup table entry with the offspring list in it.
// 	if ret != nil {
// 		return ret
// 	}
// 	return this.perform_new_value_write(block_num, entry, new_value)
// }

func (this *Slookup_i) perform_new_value_write(block_num uint32, entry *slookup_i_lib_entry.Slookup_i_entry, new_value *[]byte) tools.Ret {
	/* this handles group block writes which might involve growing or shrinking the block_group_list. */

	var ret tools.Ret
	ret = entry.Set_value(new_value) // we set this right away in the entry so whoever serializes this entry will have the correct value length
	if ret != nil {
		return ret
	}

	// get a count of how many offspring there are now in mother node
	var current_block_group_count uint32 = entry.Get_block_group_lengthxxxz()

	// figure out how many nodes we need to store this write.
	var new_value_length uint32 = 0
	if new_value != nil {
		new_value_length = uint32(len(*new_value))
	}
	var block_group_count_required uint32
	ret, block_group_count_required = this.calculate_block_group_count_for_value(new_value_length)
	if ret != nil {
		return ret
	}

	/* first handle shrinking if need be */
	this.log.Debug("current block group count: " + tools.Uint32tostring(current_block_group_count) +
		". block group count required: " + tools.Uint32tostring(block_group_count_required))
	if block_group_count_required < current_block_group_count {
		/* all the nodes past what we need get deleted and zeroed in the entry's block_group_list array */

		/* remember this problem that stree had?
		   > this physically_delete_one delete of offspring can cause anything to move, including the mother node.
		   > we must be careful to update the new location of the mother node when
		   > we go to write updates because it might have moved.
		   > So I thought this out and it's true, so we can either make the list of nodes to delete up front
		   > and just modify the list as it runs through (just like delete does) or we can re-read the mother
		   > node each time (following it if it has moved) until we've removed enough offspring nodes.
		   > I guess the first way is easier. but we still need to keep track of the movement of the mother node
		   > because I think it needs to be updated later anyway with the update we're trying to do.
			 well, don't have that problem here with slookup_i */

		// make a list of the offspring positions to delete
		var amount_to_remove uint32 = current_block_group_count - block_group_count_required
		this.log.Debug("removing: " + tools.Uint32tostring(amount_to_remove) + " blocks from block group.")

		var delete_list []uint32 = make([]uint32, amount_to_remove)
		var delete_list_pos uint32 = 0
		/* We have to delete from right to left to maintain the correctness of the block group list. when reading, we need
		 * to be able to always go from left to right until we get to a zero. If we delete from the left
		 * we'll have to set a zero in the first position (for really complicated reasons) and then
		 * there will be valid live blocks in the block group list after that and that's an invalid block group list layout
		 * so we must delete from right to left. */
		/// xxxz test for case where we're moving to a zero length block
		var rp int // must be int, because we're counting down to zero, and need to get to -1 to end loop
		for rp = int(current_block_group_count - 1); rp >= int(block_group_count_required); rp-- {
			var ret, resp = entry.Get_block_group_pos(uint32(rp))
			if ret != nil {
				return ret
			}
			var block_group_value uint32 = resp
			if block_group_value == 0 { /// xxxz remind me what this is for? this should never happen
				break
			}
			delete_list[delete_list_pos] = block_group_value
			delete_list_pos++
			this.log.Debug("adding to list to shorten block_group in entry ", entry.Get_entry_pos(),
				", remove block group position ", rp, " block: ", tools.Uint32tostring(block_group_value))
		}

		/* Now go through the delete list individually deleting each item, updating the list if
		 * something in the list got moved. also you have to remember to keep the reverse lookup list up to date */

		this.log.Debug("going to delete " + tools.Uint32tostring(delete_list_pos) + " items.")
		var dp uint32
		for dp = 0; dp < delete_list_pos; dp++ {
			var pos_to_delete uint32 = delete_list[dp]
			var ret, moved_from_resp, moved_to_resp = this.physically_delete_one(pos_to_delete)
			if ret != nil {
				return ret
			}

			/* now update the remainder of the delete list if anything in it moved. we can do the whole list,
			 * it doesn't hurt to update something that was already processed/deleted. */
			var from uint32 = moved_from_resp
			var to uint32 = moved_to_resp
			this.log.Debug("moved mover from " + tools.Uint32tostring(from) + " to " + tools.Uint32tostring(to))
			var wp int
			for wp = rp + 1; wp < int(delete_list_pos); wp++ { // as we deleted rp in this round, we don't need to update it.
				if delete_list[wp] == from {
					delete_list[wp] = to
					this.log.Debug("mover node " + tools.Uint32tostring(from) + " was in the delete list so we moved it to " + tools.Uint32tostring(to))
				}
			}
			if this.debugprint {
				this.Print(this.log)
			}
		} // loop dp

		/* so what is on disk is correct, what is in memory is probably/possibly not.
		* read the entry back in before we do anything. we have to zero out the block_group_list array entries
		* for the guys we just deleted unless delete does that already.
		* I just checked it doesn't so we have to do that here, but I think that's it.
		* 8/22/2022 so now physically_delete_one does clear out the entry's block_group_list array entries
		* so we can probably skip this.
		actally no, we can't skip this, well maybe we can. below in the else we have the case where we add
		data blocks to the entry, and that needs to be written to disk. if we don't reread from disk here
		we risk having after-the-if rewrite the entry to disk and it will be wrong if we don't refresh
		it here. so we either fix below to make sure we don't rewrite (future optimization) or
		we refresh here.
		take that back again, each code area will make sure the entry is correct on disk.
		physically delete does that for us, and add data blocks will write entry out itself
		so we must refresh here so entry is correct after this no matter what. */

		ret, entry = this.Lookup_entry_load(entry.Get_entry_pos()) // we're just reloading from disk the entry that got modified by physically delete one
		if ret != nil {
			return ret
		}
		// okay so now we have the entry back, zero out the block_group list array entries we just deleted.
		/* this was actually taken care of in physically_delete_one, so we're done already. */

		/* end of handling shrinking. */
	} else {
		/* here's the case where we add more nodes if we need them, third option is old and new size (number of block_group entries)
		   are the same, do nothing. */
		if block_group_count_required > current_block_group_count {
			// add some empty nodes and fill in the array with their position.
			var amount uint32 = block_group_count_required - current_block_group_count
			this.log.Debug("allocating " + tools.Uint32tostring(amount) + " new block_group entries to expand for update.")
			var iresp []uint32
			ret, iresp = this.allocate(amount) // this is now an stree thing not a backing store thing. xxxxxxxxxxz
			if ret != nil {
				return ret
			}

			// now peel off the new data block numbers and add them to the offspring array in the correct position.
			var i int = 0
			for rp := current_block_group_count; rp < block_group_count_required; rp++ {
				var new_data_block_pos uint32 = iresp[i]
				i++
				entry.Set_block_group_pos(rp, new_data_block_pos)
				this.log.Debug("adding data block " + tools.Uint32tostring(new_data_block_pos) + " to position " +
					tools.Uint32tostring(rp) + " in entry block_group array list.")
			}

			// write entry to disk so it is correct on disk since nobody else is going to do it later.
			if ret = this.lookup_entry_store_internal(entry.Get_entry_pos(), entry); ret != nil {
				return nil
			}

			/* and now that we know what entry those new blocks belong to, we have to also update
			   the reverse lookup entries for those blocks so lets do that here too.
			   we have to wait until after we do the entry store to disk because it is possible that the reverse lookup
			   value will need to be stored in the block we're updating. */

			i = 0
			for rp := current_block_group_count; rp < block_group_count_required; rp++ { // just for the ones we added/newly allocated
				var new_data_block_pos uint32 = iresp[i]
				i++
				// set the reverse lookup for the newly allocated data_block entries to point to this entry that they're pointed to in.
				if ret = this.reverse_lookup_entry_set(new_data_block_pos, entry.Get_entry_pos()); ret != nil {
					return ret
				}
			}

			// end of add nodes to make entry bigger because it was too small for this new value
		} else {
			this.log.Debug("node update takes the same number of offspring, no offspring change.")
		}
	} // end if resizing entry data block list. in the add case we have not yet written entry to disk

	/* so by the time we're here, in all cases, we've allocated or freed the space, set the block_group list array has
	 * all the places to put the new data, so now we just have to update the data. */

	if new_value_length == 0 { // unless of course it's empty
		this.log.Debug("new value length is zero.")
		/* this is intersting we used to have one function to update the node list and the data
		now we need two. except we actually updated the entry, we just haven't written it to disk yet.
		so we write that to disk and then we just have to actually write the data blocks */

		/* the entry has the new data value, and the correct number of block_group_data array entries to fit it,
		   we just need somebody to actually write it to disk. */
		// ahh, but this is only handling the empty value case, so there's no data to write out. */
		return nil //this.Data_block_store(entry)
	}

	/* so NOW the entry has the new data value, and the correct number of block_group_data array entries to fit it,
	   we just need somebody to actually write it to disk. */

	/* break new_value up into pieces that can fit into the pieces we allocated above for it.
	 * we're going to be updating/writing over all the data blocks in this block_group, we don't
	 * have to read update write, we know their on disk position, they've already been allocated
	 * we just write them.  */

	/* stupid.
	   we calculate this above, we don't have to do it again. */

	/* double stupid
	lookup_entry_store does all this for us. in parallel. */

	if ret = this.Data_block_store(entry); ret != nil {
		return ret
	}
	// if we got here we're good.
	return nil
}

func (this *Slookup_i) physically_delete_one(data_block_num uint32) (ret tools.Ret, moved_from uint32, moved_to uint32) {
	/* for slookup_i physically deleteing an entry is this:
		     0) the idea is we take the last allocated block (old block_num) and move it to the position being deleted (new block_num)
	       so if let's say we have a block group count of 5 that means we store 5 reverse lookup array entries in each entry.
				 so if we're going to delete block 23, that means we'd move block 29 to 23.
				 so if we were trying to find out what entry has the forward lookup for the data that is in
				 block 29, the reverse lookup entry would be in entry #5 array position 4
	block num   0 1 2 3 4   5 6 7 8 9  1011121314  1516171819  2021222324  2526272829
	entry       0 1 2 3 4 | 0 1 2 3 4 | 0 1 2 3 4 | 0 1 2 3 4 | 0 1 2 3 4 | 0 1 2 3 4 <-- this is the data_block_reverse_lookup_list

	       that array position would point to the entry number that has in it the forward lookup for the data in block 29,
				 but we don't know which block_group_list array entry we only know that it's in that entry so we have to
				 scan the block_group_list for 29. once we have that position, we can update it to point to the
				 new location 23 that 29 is moving to.
				 and then just like any normal write of that block 23, we have to update the reverse lookup for 23
				 (which is in entry #4 array pos 3) to point to entry #4 where it can be scanned for "23"

				 1) copy the data_block from the old block_num to the new block_num
				 2) do a reverse lookup on the old block_num
				 3) update the block_group_pos (scan for it) that pointed to the old block_num to point to the new block_num
				 4) write out the entry with the updated block_group array pos setting.
				 5) update the new block position's reverse lookup to point to the entry referring to that moved data_block
				    which was retrieved by the reverse lookup in step 2 and updated in step 3 and 4.
				 6) write that entry out too.
		 		 7) deallocate block. this will not overwrite the reverse lookup for that block it will be old stale
						data of out the valid range of data_blocks that exist/can be referred to,
						so that reverse lookup entry will get written over when that block is allocated again.
						we could waste the time and io to zero out that reverse lookup entry, but that's a wasted io
						and that's why we validate all the ranges all the time.
			somehow that made a lot more sense when I first wrote it.
	*/

	// get the mover data block as the last allocted block

	this.log.Debug("physically delete one at position: ", data_block_num)
	if ret = this.check_data_block_limits(data_block_num); ret != nil {
		return
	}

	var free_position uint32
	ret, free_position = this.Get_free_position()
	if ret != nil {
		return
	}
	if (free_position - 1) == data_block_num {
		// removing the last allocated block, nothing to do
		this.log.Debug("The last allocated block is being deleted, nothing to do but deallocate the last block.")
		moved_from = data_block_num // nothing is being moved, but we have to return something
		moved_to = data_block_num
		ret = this.deallocate()
		return
	}

	// return to caller what we did.
	moved_from = free_position - 1
	moved_to = data_block_num

	//	1) copy the data_block from the old block_num to the new block_num
	if ret = this.copy_data_block(moved_to, moved_from); ret != nil {
		return
	}

	// 2) do a reverse lookup on the old block_num
	var entry *slookup_i_lib_entry.Slookup_i_entry
	var block_group_pos uint32
	if ret, entry, block_group_pos = this.reverse_lookup_entry_get(moved_from); ret != nil {
		return
	}

	// 	3) update the block_group_pos that pointed to the old block_num to point to the new block_num
	if ret = entry.Set_block_group_pos(block_group_pos, moved_to); ret != nil {
		return
	}

	// 	4) write out the entry with the updated block_group array pos setting.
	if ret = this.Lookup_entry_store(entry.Get_entry_pos(), entry); ret != nil {
		return
	}

	// 	5) update the new block position's reverse lookup to point to the entry referring to that moved data_block
	// 		 which was retrieved by the reverse lookup in step 2 and updated in step 3 and 4.
	// 	6) write that entry out too.
	if ret = this.reverse_lookup_entry_set(moved_to, entry.Get_entry_pos()); ret != nil {
		return
	}

	// 	 7) deallocate block. this will not overwrite the reverse lookup for that block it will be old stale
	// 		 data of out the valid range of data_blocks that exist/can be referred to,
	// 		 so that reverse lookup entry will get written over when that block is allocated again.
	// 		 we could waste the time and io to zero out that reverse lookup entry, but that's a wasted io
	// 		 and that's why we validate all the ranges all the time.
	if ret = this.deallocate(); ret != nil {
		return
	}

	return
}

func (this *Slookup_i) copy_data_block(move_to uint32, move_from uint32) tools.Ret {
	/* read the from block of data, write it to the to block.
	this is an actual on-disk block of data, not a block_num to be looked up in the
	entry list. */
	var ret tools.Ret
	var data *[]byte
	if ret, data = this.m_transaction_log_storage.Read_block(move_from); ret != nil {
		return ret
	}
	return this.m_transaction_log_storage.Write_block(move_to, data)
}

/* allocate and deallocated used to be in the backing store, now it's here.
same basic idea though, provide the ability to acquire multiple blocks at once
but you can only free one at a time. */

func (this *Slookup_i) allocate(amount uint32) (tools.Ret, []uint32) {
	/* allocate a number of blocks (as in data blocks, not addressable blocks)
	   from free position and return an array of the positions allocated */
	/* see if there's enough room to add these nodes and if so, return their
	   positions in the array and up the free position accordingly */
	/* if we ever go concurrent we're going to have to lock this and a lot of other things I suppose */

	var lp uint32
	var rvals []uint32 = make([]uint32, amount)
	for lp = 0; lp < amount; lp++ {

		var ret, free_position = this.Get_free_position()
		if ret != nil {
			return ret, nil
		}
		rvals[lp] = free_position
		free_position += 1
		if ret = this.Set_free_position(free_position); ret != nil {
			return ret, nil
		}
	}
	return nil, rvals
}

func (this *Slookup_i) deallocate() tools.Ret {
	/* slookup_i level deallocate last allocated block.
	lower the free position by one, write the header to disk
	call m_storage deallocate. */

	var ret, free_position = this.Get_free_position()

	if ret != nil {
		return ret
	}
	free_position = free_position - 1
	if ret = this.Set_free_position(free_position); ret != nil {
		return ret
	}

	if ret = this.m_header.Store_header(this.log, this.m_transaction_log_storage, false); ret != nil {
		return ret
	}

	// if we've shrunk, which we have, tell backing store that it can shrink if it wants

	if ret = this.m_storage.Mark_end(free_position); ret != nil {
		return ret
	}

	return nil
}

// this is a handy call to discard for a block which writes zero data which causes allocated block_group data to be deleted for that block */

func (this *Slookup_i) Discard(block_num uint32) tools.Ret {
	return this.Write(block_num, nil)
}

// this is a main entrypoint for zosbd2_slookup_i backing_store

func (this *Slookup_i) Write(block_num uint32, new_value *[]byte) tools.Ret {
	/* this function will write a block. */
	/* this is not where the transaction should be I think. it should be higher up so a large multiblock write can all be in one transaction.
	   then again, each block write should yield a transaction's worth of idempotent log information, because a write can shrink a block and
		 cause deletes and so on, so a transaction for a single block write is still useful. we could do better though. */
	this.interface_lock.Lock()
	defer this.interface_lock.Unlock()

	var ret tools.Ret
	if ret = this.m_transaction_log_storage.Start_transaction(); ret != nil {
		return ret
	}
	defer this.m_transaction_log_storage.End_transaction()
	if ret := this.write_internal(block_num, new_value); ret != nil {
		return ret
	}
	// if we got here okay, commit the transaction.
	this.m_transaction_log_storage.Set_commit()
	return nil
}

func (this *Slookup_i) write_internal(block_num uint32, new_value *[]byte) tools.Ret {
	/* way simpler than stree, we just write the block */

	var ret tools.Ret
	var entry *slookup_i_lib_entry.Slookup_i_entry
	if ret, entry = this.Lookup_entry_load(block_num); ret != nil {
		return ret
	}
	return this.perform_new_value_write(block_num, entry, new_value)
}

/* this is a main zosbd2 entry point. fetch a block */

func (this *Slookup_i) Read(block_num uint32) (Ret tools.Ret, resp *[]byte) {
	this.interface_lock.Lock()
	defer this.interface_lock.Unlock()

	this.m_transaction_log_storage.Start_transaction()
	defer this.m_transaction_log_storage.End_transaction()
	var respdata *[]byte
	var ret tools.Ret
	if ret, respdata = this.read_internal(block_num); ret != nil {
		return ret, nil
	}
	// if we got here okay, commit the transaction. do we need to commit on read transactions? there's a good reason for this but I forget what it is. I guess we'll find out.
	this.m_transaction_log_storage.Set_commit()

	return nil, respdata
}

func (this *Slookup_i) read_internal(block_num uint32) (tools.Ret, *[]byte) {

	var ret tools.Ret
	var entry *slookup_i_lib_entry.Slookup_i_entry
	ret, entry = this.Lookup_entry_load(block_num)
	if ret != nil {
		return ret, nil
	}
	// now get the data
	var respdata *[]byte
	if ret, respdata = this.Data_block_load(entry); ret != nil {
		return ret, nil
	}

	///xxxxz make sure this returns the actual length expected
	return nil, respdata
}

/* the rest of this file is filled with all the crazy complicated stuff needed to make delete work in all cases
for stree. slookup_i is soooooo much way simpler we're not even going to look at it, we're just going to implement
delete and not even question what stree did. we should probably look through it for all the goofy edge cases
I came across, but other than that it won't help us much. */

// func (this *Slookup_i) physically_delete(pos uint32) tools.Ret {
// 	/* step 2, is just physically copy the data from the last node to the hole,
// 	 * then find the parent of the moved node and repoint it to the newly filled hole,
// 	 * then the children's parents must also be set to its new location, the newly
// 	 * filled hole. */

// 	/* 11/6/2020 so physically deleting a node is a bit more interesting with offspring.
// 	 * if the node is a mother node, we have to delete all the offspring, and the mother node.
// 	 * makes sense to do it in reverse in case they're all at the end, deleting is easier.
// 	 * if deleting an offspring node, we would have to update the parent, but we know it's going away
// 	 * so we don't have to do that, all we do have to do is if we're MOVING an offspring node, update
// 	 * the parent's offspring array to point to the new location.
// 	 * It sounds a bit risky that we are going to recurse a bit when we delete a mother node because
// 	 * we have to physically delete all the offspring nodes, but if the tree isn't corrupt, it should
// 	 * work out fine. Famous last words. */

// 	/* so here is where we care if it's a mother node or offspring being deleted we can take care of two things.
// 	 * if it's a mother node the rewrite of the ones that point to it are the same as stree_iii
// 	 * with the added caveat that we have to go through the effort of deleting the offspring as well.
// 	 * and if it's an offspring node being moved, rewriting the parent is a little different.
// 	 * actually we have to think about this a little more.
// 	 * deleting a mother node just means we have to delete the offspring
// 	 * but keep in mind that deleting offspring might cause the mother node to move.
// 	 * so we can't just delete this position we got because it might be different after we do the offspring
// 	 * so maybe order is important.
// 	 * I keep missing that there's an important distinction between the nodes that are moving and the one(s)
// 	 * being deleted. it was a lot simpler when there was only one thing being deleted.
// 	 * so let's go through all the combinations.
// 	 *
// 	 * by the time physically delete is called, the mother node is not in the binary search tree anymore
// 	 * it is just taking up space on disk. The offspring of that mother node were never in the tree and as
// 	 * soon as the mother node is gone there's no more reference to the offspring nodes.
// 	 * physically deleting nodes however can cause anything to move, so we have to be careful
// 	 * if we make a list of what to delete because items in that list can get stale after one of them
// 	 * is deleted. so I think what has to happen is that we DO have to update the mother node as offspring
// 	 * are deleted so that we can always ask the mother node for currently correct things to delete.
// 	 * that means
// 	 * a) we have to physically delete offspring first
// 	 * b) we have to phyiscally delete offspring from back to front (well we don't HAVE to but it keeps the list
// 	 * consistent and we have to blank out the offspring list nodes as we delete them
// 	 * c) we have to keep an eye on the mother node location because it is possible that it can move as a
// 	 * result of deleting one of its offspring.
// 	 * I don't think there's a way around c.
// 	 * if we delete the mother first then we have no way of keeping track of if the other offspring moved as
// 	 * a result of other offsprings nodes physically deleted earlier.
// 	 * the only thing we can rely on is the mother node, and I guess the only way to be sure is
// 	 * to make sure we keep track if the mother node moved as a result of deleting an offspring node.
// 	 * one way to cheap out would be to say if the mother node is the last thing in the list
// 	 * then move it manually (swap with the one before it) so that it will not get moved out from under us
// 	 * unexpectedly, but I'm not sure that's any easier than just keeping track of if it moves because of an
// 	 * offspring node delete.
// 	 * then there's the problem of if the mother node is the last one but I guess it can't be since it gets
// 	 * deleted only after its offspring is deleted.
// 	 * so in summary, I think we just check if we're deleting a mother node, delete the offspring first
// 	 * have that update the mother node as it happens, and somehow report if the mother node got moved
// 	 * as a result. deleting is hard. testing will be hard too. */

// 	/* 11/9/2020 okay so I drew it out and worked it all out, and it's not that bad.
// 	   * The short of it is we need the parent function that handles physical deletes of
// 	   * mother nodes, to generate calls to a function that physically deletes one like we used to.
// 	   * the trick is to keep track of what got moved where so that if the original list contains
// 	   * the item moved, its number gets updated so the newly moved position gets deleted.
// 	                   deleteing m1 and offspring o2 o3 o4...
// 	                    1  2  3  4  5  6  7        delete list
// 	                A   m o2 o3 o4 m1 m5 o6          4 3 2 5
// 	                B   m o2 o3 o6 m1 m5      7->4   x 3 2 5
// 	                C   m o2 m5 o6 m1         6->3   x x 2 5
// 	                D   m m1 m5 o6            5->2   x x x 2  (because 5 moved to 2 so we change 5 to 2)
// 	                E   m o6 m5               4->2   x x x x

// 	     another example

// 	                   deleteing m1 and offspring o1 o2 o3...
// 	                    1  2  3  4  5              delete list
// 	                A   m o1 o2 o3 m1                4 3 2 5
// 	                B   m o1 o2 m1            5->4   x 3 2 4  (because 5 moved to 4 so we change 5 to 4)
// 	                C   m o1 m1               4->3   x x 2 3  (because 4 moved to 3 so we change 4 to 3)
// 	                D   m m1                  3->2   x x x 2  (because 3 moved to 2 so we change 3 to 2)
// 	                E   m                     2->2   x x x x
// 	*/
// 	/* caller already loaded the node from which we can get the list of offspring to delete */
// 	/* 11/20/2020 so I dunno WHY the caller loaded the node and passed it to me, but it loaded it
// 	 * before it was logically deleted, and therefore is stale, we reload the modified mother node here. */

// 	var ret, noderesp = this.Node_load(pos)
// 	if ret != nil {
// 		return ret
// 	}
// 	var mothertodelete stree_v_node.Stree_node = *noderesp

// 	if mothertodelete.Is_offspring() {
// 		return tools.Error(this.log, "request to delete an offspring node not allowed.")
// 	}

// 	// copy the list of offspring so we have what to delete
// 	var delete_list []uint32 = make([]uint32, this.m_offspring_per_node+1) // need to including deleting of mother node

// 	var delete_list_pos int = 0
// 	//        for (int lp = 0; lp < m_offspring_per_node; lp++) // go to end of list, it might not end in zero
// 	/* 11/20/2020 this was a problem, we used to delete offspring from left to right, but that doesn't work
// 	 * because physically_delete_one stops fixing the offspring list when it gets to a zero from left to
// 	 * right, so we must delete in right to left order, just like shrink does so physically delete
// 	 * will always have a correct offspring list in mother node to work with.
// 	 * okay so that works, if all offspring are full, or rather it did when we went from left to right.
// 	 * Now we have to go from right to left, but we have to start either at the end of the offspring list
// 	 * or don't use zero as the end of the list because we're not going left to right we're going right to
// 	 * left and if not all offspring are used, the last one will be empty/zero and we will bail right away.
// 	 * so we romp through the whole offspring list backwards, and only add non-zero things, but don't stop short. */
// 	var rp int
// 	for rp = int(this.m_offspring_per_node) - 1; rp >= 0; rp-- { // delete them in reverse just like shrink does

// 		var ret, resp = mothertodelete.Get_offspring_pos(uint32(rp))
// 		if ret != nil {
// 			return ret
// 		}
// 		var offspring_value uint32 = *resp
// 		if offspring_value == 0 {
// 			continue
// 		}
// 		delete_list[delete_list_pos] = offspring_value
// 		delete_list_pos++
// 		this.log.Debug("removing node: ", offspring_value)
// 	}
// 	/* we must delete the mother node last because in deleting the offspring
// 	 * nodes it will update the parent with the new offspring list
// 	 * with the deleted one zeroed out, so the mother must be around to
// 	 * note all of its offspring going away. */
// 	delete_list[delete_list_pos] = pos // the mother node to delete
// 	delete_list_pos++
// 	this.log.Debug("removing mother node: ", pos)

// 	/* Now go through the delete list individually deleting each item, updating the list if
// 	 * something in the list got moved. */
// 	this.log.Debug("going to delete ", delete_list_pos, " items.")
// 	for rp := 0; rp < delete_list_pos; rp++ {
// 		var pos_to_delete uint32 = delete_list[rp]

// 		var ret, moved_resp_from, moved_resp_to = this.physically_delete_one(pos_to_delete)
// 		if ret != nil {
// 			return ret
// 		}
// 		/* now update the remainder of the list if anything in it moved. we can do the whole list,
// 		 * it doesn't hurt to update something that was already processed/deleted. */
// 		var from uint32 = moved_resp_from
// 		var to uint32 = moved_resp_to
// 		this.log.Debug("moved mover from ", from, " to ", to)

// 		for wp := rp + 1; wp < delete_list_pos; wp++ { // as we deleted lp in this round, we don't need to update it.

// 			if delete_list[wp] == from {
// 				delete_list[wp] = to
// 				this.log.Debug("mover node ", from, " was in the delete list so we moved it to ", to)
// 			}
// 		}
// 		// only for small trees this.print(); // xxxz
// 	}
// 	return nil
// }

// func (this *Slookup_i) clean_deleted_offspring_from_mother(toremove_pos uint32) tools.Ret {
// 	// caller already loaded pos into toremove, this is a double read, but that's what caches are for.

// 	var ret, resp = this.Node_load(toremove_pos)
// 	if ret != nil {
// 		return ret
// 	}
// 	var toremove stree_v_node.Stree_node = *resp
// 	if toremove.Is_offspring() == false {
// 		return tools.Error(this.log,
// 			"sanity failure, offspring was told to clean itself out of mother but it is not an offspring node.")
// 	}

// 	// get our mother.
// 	ret, resp = this.Node_load(toremove.Get_parent())
// 	if ret != nil {
// 		return ret
// 	}
// 	var mother *stree_v_node.Stree_node = resp
// 	if mother.Is_offspring() != false {
// 		return tools.Error(this.log, "sanity failure, deleting offspring node who's parent is not a mother node.")
// 	}
// 	var found bool = false
// 	/* go through all the mother's offspring, find ourselves and erase us from the list.
// 	 * deletes all work from right to left in the offspring list, so this should leave a correct offspring list */
// 	var rp uint32
// 	for rp = 0; rp < this.m_offspring_per_node; rp++ {

// 		var ret, offspring_resp = mother.Get_offspring_pos(rp)
// 		if ret != nil {
// 			return ret
// 		}
// 		var offspring_peek uint32 = *offspring_resp
// 		if offspring_peek == 0 { // end of list
// 			break
// 		}
// 		if offspring_peek == toremove_pos {
// 			found = true
// 			mother.Set_offspring_pos(rp, 0) // remove it
// 			break                           // there can (better) be only one, and it better be the last one too.
// 		}
// 	}
// 	if found == false {
// 		return tools.Error(this.log, "sanity failure, tree is corrupt, while physically deleting ", toremove_pos,
// 			" we tried find ourselves in our mother's offspring list, but we didn't find ourselves.")
// 	}
// 	ret = this.node_store(toremove.Get_parent(), mother)
// 	if ret != nil {
// 		return ret
// 	}
// 	return nil
// }

// func (this *Slookup_i) physically_delete_one(pos uint32) (tools.Ret /* moved_resp_from */, uint32 /* moved_resp_to*/, uint32) {
// 	/* 11/9/2020 Okay, now that we've separated it into two parts, it's not that bad.
// 	 * the first part figures out what to delete and updates the list if things got moved,
// 	 * and here, we just delete one. deleting the mother node is same as always, we have to update
// 	 * any parents and children that pointed to the thing that moved into our (the deleted node's) place.
// 	 * almost forgot, if we're the mother node, we also have to update all of our offspring to say we (pos)
// 	 * is the new location of mover's parent.
// 	 * deleting an offspring node involves just updating the mother that points to us.
// 	 * The other important change is that we have to return the two value of from and to positions
// 	 * that were moved, so caller can update the list of things to delete appropriately if things
// 	 * in the list to delete get moved while in here. */

// 	/* When deleting a mother node we know we're going to delete all the offspring and the mother node
// 	 * so we don't have to update anything, as all members of this node, mother and offspring will be
// 	 * overwritten soon enough.
// 	 * But in the case where we are shrinking a node because we are updating an existing node
// 	 * with less data that needs fewer offspring, we will be deleting offspring and NOT
// 	 * deleting the mother node, so we do in fact have to update the mother node if things got
// 	 * moved.  wait maybe not. no, I'm being dumb, remember, when we delete something it goes away
// 	 * we only update things related to the mover node, not the node being deleted.
// 	 * in the case of shrinking a node, we are simply deleting offspring nodes
// 	 * that nobody will ever refer to again, the zeroing out of the parent happens in the
// 	 * update/shrinking function and here we deal with moving important nodes into its place
// 	 * and all the correct updates are performed. all is well.
// 	 * that's funny, in update/shrink I said I zeroed out the mother node offspring entries here, and here
// 	 * I say that I did it there. doing it there makes more sense. don't need to do it on every delete
// 	 * just offspring deletes where we don't delete the parent which only happens in update/shrink. */

// 	/* 12/23/2020 I think I figured out the bad root node bug. when we move mother nodes around we shuffle
// 	 * all of the parents and children and stuff, but if the mother node being moved is the root node
// 	 * we have to update the logically deleted root node header, and I think we missed doing that. */

// 	if pos == 0 {
// 		return tools.Error(this.log, "sanity failure, tree is corrupt, physically delete one asked to delete node zero."), 0, 0
// 	}

// 	var moved_resp_from uint32
// 	var moved_resp_to uint32

// 	this.log.Debug("physically delete one at position: ", pos)
// 	var iresp uint32
// 	var ret tools.Ret
// 	ret, iresp = this.Get_free_position()
// 	if ret != nil {
// 		return ret, 0, 0
// 	}
// 	var free_position uint32 = iresp
// 	if (pos == 1) && (free_position == 2) { // deleted the root node
// 		// removing the last remaining node, nothing to do
// 		moved_resp_from = pos // nothing is being moved, but we have to return something
// 		moved_resp_to = pos

// 		/* 11/20/2020 when you physically delete something, if it was a mother, no big deal, it has already
// 		 * been logically deleted  but if you're deleting an offspring, you still have to find your mother
// 		 * and tell her you're gone otherwise when the mother goes to move or get deleted, it is pointing
// 		 * to a node that is gone or at least isn't hers. This was a quick out for the simple delete case,
// 		 * but there are no simple delete cases. */
// 		/* okay I meant this for below, in this case, it actually is simple, if you're deleting the root node
// 		 * then it must be a mother, just deallocate it. logically delete would have already set the root
// 		 * node to 0 by the time we get here. */
// 		return this.m_storage.Deallocate(), moved_resp_from, moved_resp_to
// 	}
// 	if free_position == 2 {
// 		return tools.Error(this.log,
// 			"tree is corrupt, free position is 2, but delete request is not to delete position 1, but position: ",
// 			pos), 0, 0 // something wrong here.
// 	}

// 	/* Special case, if we're removing the last item, do nothing but lower
// 	 * the free position, the array item has already been orphaned and
// 	 * nothing is pointing to it. For recovery's sake it might be worth zeroing out the newly deleted element
// 	 * because if we search for the end manually we might pick up a deleted node and then we're screwed forever.
// 	 * 11/9/2020 we're not going to do recovery that way, we're going to do it with a transaction log, so don't worry
// 	 * about zeroing out deleted nodes. */
// 	/* this is true for orphans too, I think, it depends on the order in which we delete a node with offspring.
// 	   regardless of order all the nodes mother and offspring will go away, so once the node is logically
// 	   deleted we really just need to remove the nodes and we don't have to update the mother because it will
// 	   be going away too. so when we physically delete a node we just have to worry about the one we're moving into
// 	   its place. the node being deleted and its mother don't matter at all. */

// 	// so that last bit is not entirely true...

// 	/* but wait! we can't overwrite pos yet, because pos might be an offspring and if we're overwriting it
// 	 * we have to update it's mother to remove the offspring from the mother's list.
// 	 * I don't think anybody else takes care of that, logically delete doesn't, it only works on mother
// 	 * node links. So I believe we have to do that here. */
// 	/* There are two callers of this, shrink and delete. Shrink does take care of cleaning out the mother's offspring list
// 	 * but I don't think delete does, which is how we found this problem in the first place, when the simple remove
// 	 * the offspring off the end case didn't clean itself up in the mother. So same thing, delete doesn't do it
// 	 * for the complex case either, which means we have to remove the cleaning from shrink since it's done here
// 	 * because it will fail if we clean it and shrink tries to because shrink won't find it. Actually
// 	 * shrink knew what it was doing and just blindly cleared out the offspring list, but since we're
// 	 * taking care of it here, I removed it from shrink. save us a read. */

// 	/* HERE is where we can't simply deallocate, we have to update the mother if we're an offspring. */
// 	// first see if it's an offspring or not, this will cause a double load, but that's what caches are for.
// 	/* So it turns out we need to do this in all cases, whether the node being removed is at the end or
// 	 * not so just do it up front here. Go find the about-to-be-deleted-node's mother and update it. */

// 	var resp *stree_v_node.Stree_node
// 	ret, resp = this.Node_load(pos)
// 	if ret != nil {
// 		return ret, 0, 0
// 	}
// 	var toremove stree_v_node.Stree_node = *resp
// 	if toremove.Is_offspring() {
// 		/* we haven't overwritten anything yet so we can still load and find its mother.
// 		 * surprisingly, as similar as all these fixups are, none are identical, sharing
// 		 * code is risky because it's confusing enough to follow what's going on as it is,
// 		 * without introducing a bunch of "if this mode, do it slightly differently" so
// 		 * for now, we handle this case right here. well, since this isn't rust we can
// 		 * do it in a function. */
// 		var ret = this.clean_deleted_offspring_from_mother(pos)
// 		if ret != nil {
// 			return ret, 0, 0
// 		}
// 	}

// 	if pos == free_position-1 {
// 		this.log.Debug("deleted item is in last position, just deallocating.")
// 		moved_resp_from = pos // nothing is being moved, but we have to return something
// 		moved_resp_to = pos
// 		return this.m_storage.Deallocate(), moved_resp_from, moved_resp_to
// 	}

// 	// nothing in mover has to change, just the people pointing to mover have to change
// 	var mover_pos uint32 = free_position - 1

// 	// now we know what we're moving where, make sure we return it to caller.
// 	moved_resp_from = mover_pos
// 	moved_resp_to = pos

// 	if this.debugprint {
// 		this.diag_dump_one(mover_pos)
// 	}

// 	ret, resp = this.Node_load(mover_pos)
// 	if ret != nil {
// 		return ret, 0, 0
// 	}
// 	var mover *stree_v_node.Stree_node = resp

// 	/* for both mother and offspring, copy the data into the correct array location.
// 	 * actually, that worked fine when the node was simpler but now I think it just
// 	 * makes more sense to read the mover node and write it into the movee's location (pos),
// 	 * that way we don't have to worry about missing anything regardless of type (mother or offspring) */

// 	/* this is the first important bit, we write m's data over the old n's location (pos) */
// 	/* okay now 11/16/2020 we're doing this at the end because the act of writing mother over it's new
// 	 * location can possibly overwrite the offspring it's deleting which makes updating
// 	 * the mother's offspring's parents difficult, so since we have to update the mother's
// 	 * offspring list (setting the offspring to zero if its in there) we will save writing
// 	 * for last. */

// 	/* another explanation of the sinister problem.
// 	 * If we just copied the mother node over one of the mother's offspring
// 	 * that we are deleting, the attempt to update the offspring's parent will corrupt the
// 	 * tree because the mother will think it's updating an offspring node, when in fact
// 	 * it will be updating itself. */

// 	// now update all the people pointing to mover to point to pos instead.
// 	/* This is where mother/offspring type matters */

// 	if mover.Is_offspring() {
// 		/* mover node is offspring, go get our parent, find mover in the offspring
// 		 * list and change it to pos */

// 		/* since we had to move the actual mover node store/write to the end of if-mother, we have to remember to do it
// 		 * for if-offspring nodes too, it doesn't affect anything about the mover node, since it's an offspring
// 		 * only things pointing to it matter, so just write it out now. */

// 		var ret = this.node_store(pos, mover)
// 		if ret != nil {
// 			return ret, 0, 0
// 		}

// 		ret, resp = this.Node_load(mover.Get_parent())
// 		if ret != nil {
// 			return ret, 0, 0
// 		}
// 		var mother *stree_v_node.Stree_node = resp
// 		if mother.Is_offspring() != false {
// 			return tools.Error(this.log, "sanity failure, deleting offspring node who's parent is not a mother node."), 0, 0
// 		}

// 		var found bool = false
// 		var rp uint32
// 		for rp = 0; rp < this.m_offspring_per_node; rp++ {

// 			var ret, offspring_resp = mother.Get_offspring_pos(rp)
// 			if ret != nil {
// 				return ret, 0, 0
// 			}

// 			var offspring_peek uint32 = *offspring_resp
// 			if offspring_peek == 0 { // end of list
// 				break
// 			}
// 			if offspring_peek == mover_pos {
// 				found = true
// 				mother.Set_offspring_pos(rp, pos)
// 				break // there can (better) be only one
// 			}
// 		}
// 		if found == false {
// 			return tools.Error(this.log,
// 				"sanity failure, tree is corrupt, while physically deleting ", pos, " we tried to move ",
// 				mover_pos, " to it, mover ", mover_pos, " was an offspring node, but its parent ",
// 				mover.Get_parent(), " does not point to mover"), 0, 0
// 		}

// 		ret = this.node_store(mover.Get_parent(), mother)
// 		if ret != nil {
// 			return ret, 0, 0
// 		}
// 	} else { // mover node is a mother node, update all the tree pointers to point from mover to pos */

// 		// first update mover's parent, either root or parent's children should point to pos
// 		/* 11/20/2020 will problems never wane...
// 		 * so in this case:
// 		 * -- (wx) bb (ZYXW) 10 (stuv)
// 		 *      bb
// 		 *     /
// 		 *    10
// 		 * if we're deleting 10, first we logically delete it, then we, here, physically
// 		 * delete node 1 (10's offspring) and we do that by moving 10 (node 3) to node 1.
// 		 * then because 10 is a mother node, we try and update its parent's children pointers
// 		 * and its children's parent pointers.
// 		 * But if we were just bb's left child which we were, remember we first logically
// 		 * deleted it to remove it from the tree
// 		 * so we are no longer bb's child and that's okay. but our parent still says bb.
// 		 * we can't trigger an error here if we can't
// 		 * find ourselves as one of our parent's kids, because of this case.
// 		 * basically if we are an orphaned mother node, it is okay to not update any pointers
// 		 * we logically do not exist, do not have childen or a parent, nothing to update. */
// 		var parent uint32 = mover.Get_parent()
// 		if parent == math.MaxUint32 {
// 			// if parent is max_value then this mover was just deleted as part of the logical delete for which we are the physical delete.
// 			this.log.Debug("mother node being moved because of physical delete was logically deleted and is an orphan")
// 		} else {
// 			// check for pos being the root node
// 			if mover.Get_parent() == 0 {
// 				/* this is our 12/23/2020 bug. We can't rely on the mother node's parent being zero to tell
// 				 * us if it is the root node. It may be the one being deleted, and therefore is already
// 				 * logically deleted, so we should not update the root node in that case. We took care of
// 				 * that problem for all other mother's being moved by setting the sentinal value so we
// 				 * know not to update pointers of logically deleted nodes. But we missed this one case, where
// 				 * the one being deleted is also the root node. */

// 				var ret, rootnoderesp = this.m_storage.Get_root_node()
// 				if ret != nil {
// 					return ret, 0, 0
// 				}
// 				/* so if mover thinks it is the root node, AND the root node thinks mover is the
// 				 * root node, only then do we update it. If this mother node which WAS the root node
// 				 * got logically deleted and is now being physically deleted, we don't update the root node. */
// 				var root_node_pos uint32 = rootnoderesp
// 				if root_node_pos == mover_pos {
// 					ret = this.m_storage.Set_root_node(pos)
// 					if ret != nil {
// 						return ret, 0, 0
// 					}
// 				}
// 			} else {
// 				ret, resp = this.Node_load(mover.Get_parent())
// 				if ret != nil {
// 					return ret, 0, 0
// 				}
// 				var p *stree_v_node.Stree_node = resp
// 				if p.Get_left_child() == mover_pos {
// 					p.Set_left_child(pos)
// 				} else {
// 					if p.Get_right_child() == mover_pos {
// 						p.Set_right_child(pos)
// 					} else {
// 						if this.debugprint {
// 							this.diag_dump_one(mover.Get_parent())
// 						}
// 						return tools.Error(this.log,
// 							"sanity failure, tree is corrupt, moving mother node ", mover_pos,
// 							" but neither of mother's parent's ", mover.Get_parent(), " children ",
// 							p.Get_left_child(), " and ", p.Get_right_child(), " point to mover ", mover_pos), 0, 0
// 					}
// 				}
// 				var ret = this.node_store(mover.Get_parent(), p)
// 				if ret != nil {
// 					return ret, 0, 0
// 				}
// 			}
// 		} // if moving mother node is live in the tree and should have its pointers updated.

// 		// second update mover's children, if any, set their parent to pos
// 		/* 11/20/2020 this has the same problem as above if the node that's being moved is the mother
// 		 * node that is being deleted as part of this physically moving delete of an offspring
// 		 * we can't try and set our children's mother because our children are not our children anymore.
// 		 * so I think when we logically delete a mother node, we should set its parent and children
// 		 * to sentinal values so that when we get here, in case of move, we don't try and update them.
// 		 * this way we can still do validity checks on the ones that should be moving that aren't deleted. */
// 		var left_child uint32 = mover.Get_left_child()
// 		if left_child == math.MaxUint32 { // if left_child is max_value then this mover was just deleted as part of the logical delete for which we are the physical delete.
// 			this.log.Debug("mother node being moved because of physical delete was logically deleted and is an orphan") // we will see this message 3 times.
// 		} else {
// 			if left_child != 0 {
// 				ret, resp = this.Node_load(mover.Get_left_child())
// 				if ret != nil {
// 					return ret, 0, 0
// 				}
// 				var lc *stree_v_node.Stree_node = resp
// 				lc.Set_parent(pos)
// 				var ret = this.node_store(mover.Get_left_child(), lc)
// 				if ret != nil {
// 					return ret, 0, 0
// 				}
// 			}
// 		}
// 		var right_child uint32 = mover.Get_right_child()
// 		if right_child == math.MaxUint32 { // if right_child is max_value then this mover was just deleted as part of the logical delete for which we are the physical delete.
// 			this.log.Debug("mother node being moved because of physical delete was logically deleted and is an orphan") // we will see this message 3 times.
// 		} else {
// 			if mover.Get_right_child() != 0 {
// 				var ret, resp = this.Node_load(mover.Get_right_child())
// 				if ret != nil {
// 					return ret, 0, 0
// 				}
// 				var rc *stree_v_node.Stree_node = resp
// 				rc.Set_parent(pos)
// 				ret = this.node_store(mover.Get_right_child(), rc)
// 				if ret != nil {
// 					return ret, 0, 0
// 				}
// 			}
// 		}
// 		/* lastly, now we have to go to all of our offspring and tell them their parent is now pos */
// 		/* 11/20/2020 in the case of the orphaned mother node, this is still true, we are only logically
// 		 * orphaned from the tree, our offspring are still our problem and must be kept up to date until
// 		 * they are deleted. Remember in this case, we are a logically orphaned mother node that got moved BECAUSE
// 		 * our offspring got physically deleted. */
// 		var rp uint32
// 		for rp = 0; rp < this.m_offspring_per_node; rp++ {

// 			var offspring_resp *uint32
// 			ret, offspring_resp = mover.Get_offspring_pos(rp)
// 			if ret != nil {
// 				return ret, 0, 0
// 			}
// 			var offspring_node uint32 = *offspring_resp
// 			if offspring_node == 0 { // end of list
// 				break
// 			}
// 			/* if we happen come across the offspring being deleted, zero it out
// 			 * and especially do not try and update its parent. This is that sinister
// 			 * problem I was talking about where a mother is moving over its own offspring
// 			 * that's being deleted. */
// 			if offspring_node == pos {
// 				mover.Set_offspring_pos(rp, 0)
// 				continue
// 			}

// 			var ret, resp = this.Node_load(offspring_node)
// 			if ret != nil {
// 				return ret, 0, 0
// 			}
// 			var o *stree_v_node.Stree_node = resp
// 			o.Set_parent(pos)
// 			ret = this.node_store(offspring_node, o)
// 			if ret != nil {
// 				return ret, 0, 0
// 			}
// 		} // for

// 		/* so now that we've updating all the mother's offspring and set the offspring array if we are overwriting
// 		 * one of our own offspring, we can finally rewrite the mother node to disk in it's correct final place. */
// 		var ret = this.node_store(pos, mover)
// 		if ret != nil {
// 			return ret, 0, 0
// 		}

// 	} // if mover is a mother node

// 	// remove old mover position from the allocated array list.
// 	return this.m_storage.Deallocate(), moved_resp_from, moved_resp_to
// }

// slookup has no concept of delete, there is nothing to delete, there is trim/discard
// but no actual delete.

// func (this *Slookup_i) Delete(key string, not_found_is_error bool) tools.Ret {
// 	// if you want to print the pre-delete tree.
// 	//        ArrayList<Integer> iresp = new ArrayList<Integer>();
// 	//        String Ret = this.storage.get_root_node(iresp);
// 	//          if ret != nil {
// 	//          return Ret;
// 	//        int root_node = iresp.get(0);
// 	//        treeprinter_iii.printNode(this, root_node);

// 	this.interface_lock.Lock()
// 	defer this.interface_lock.Unlock()

// 	var ret, respfound, _, respnodepos = this.search(key, false)
// 	if ret != nil {
// 		return ret
// 	}

// 	var found bool = respfound
// 	var pos uint32 = respnodepos
// 	if found == false {
// 		if not_found_is_error {
// 			return tools.Error(this.log, syscall.ENOENT, "no node found to delete for key: ", key) // can't delete what we can't find.
// 		}
// 		// this.log.Debug("no node found to delete for key: ", key) // this is rather noisy for discard
// 		return nil
// 	}
// 	/* two steps, first we have to logically delete the node, then
// 	 * we have to physically move something into its place in storage. */
// 	/* regardless of what happens during logical delete, it's only pointers
// 	 * that move around, when it comes to step 2 to physically delete,
// 	 * we're always going to be deleting the pos array entry, even
// 	 * if it's the root node and it's the only one left. */
// 	ret = this.logically_delete(pos)
// 	if ret != nil {
// 		return ret
// 	}
// 	ret = this.physically_delete(pos)
// 	if ret != nil {
// 		return ret
// 	}

// 	// if you want to print the post-delete tree.
// 	//        Ret = this.storage.get_root_node(iresp);
// 	//          if ret != nil {
// 	//          return Ret;
// 	//        root_node = iresp.get(0);
// 	//        treeprinter_iii.printNode(this, root_node);
// 	return nil
// }

// /* in java there is a package scope, but I don't think go has that, so it's public. */
// func (this *Slookup_i) Get_root_node() uint32 {
// 	// only used for treeprinter, package scope

// 	var ret, iresp = this.m_storage.Get_root_node()
// 	if ret != nil {
// 		fmt.Println(ret)
// 		return 0
// 	}
// 	return iresp
// }

// func (s *Stree_v) Load(pos uint32) *stree_v_node.Stree_node { // only used for testing, package scope

// 	var ret, resp = s.Node_load(pos)
// 	if ret != nil {
// 		fmt.Println(ret)
// 		return nil
// 	}
// 	return resp
// }

// this all came from the java generics figure-out-the-type-size stuff which doesn't apply to go

// func Calculate_block_size(log *tools.Nixomosetools_logger, key_type string, value_type []byte,
// 	max_key_length uint32, max_value_length uint32, additional_offspring_nodes uint32) (ret tools.Ret, resp uint32) {
// 	/* for startup, the caller doesn't know how big we're going to make a block, so it can use this to ask us.
// 	 * They don't know what offspring nodes are and how we can store as much data as node_size * offspring_nodes + 1
// 	 * so they just pass us the number of nodes total they want to store, and we subtract accordingly.
// 	 so for the go version, we're not doing the +1/-1 thing. */
// 	var n *stree_v_node.Stree_node = stree_v_node.New_Stree_node(log, key_type, value_type, max_key_length, max_value_length,
// 		additional_offspring_nodes)
// 	// we pass the max field size because that's what we determine block size with.
// 	var serialized_size uint32 = n.Serialized_size(max_key_length, max_value_length)

// 	return nil, uint32(serialized_size)
// }

func (this *Slookup_i) Get_used_blocks() (tools.Ret, uint32) {
	this.interface_lock.Lock()
	defer this.interface_lock.Unlock()
	var free_position uint32
	var ret tools.Ret
	if ret, free_position = this.Get_free_position(); ret != nil {
		return ret, 0
	}

	var start = this.Get_first_data_block_start_block()
	var used = free_position - start
	return nil, used
}

func (this *Slookup_i) Get_total_blocks() uint32 {
	/* this is the total number of blocks in the backing store.
	   if there are 500 blocks in the store, block 0 is the header, 10 is the start of
		 the lookup table, 80 is the start of the transaction log, 100 is the start
		 of the data blocks, 500 is the end of the data blocks, then
		 we return 500 because there are a total of 500 data blocks of storable space. */

	return this.m_header.M_total_backing_store_blocks
}

func (this *Slookup_i) Get_total_data_blocks() uint32 {
	/* this is the total number of blocks available to store data in the backing store.
	   if there are 500 blocks in the store, block 0 is the header, 10 is the start of
		 the lookup table, 80 is the start of the transaction log, 100 is the start
		 of the data blocks, 500 is the end of the data blocks, then
		 we return 400 because there are a total of 400 data blocks of storable space for data. */

	var total_data = this.m_header.M_total_backing_store_blocks - this.Get_first_data_block_start_block()
	return total_data
}

func (this *Slookup_i) Diag_dump_slookup_header() {
	this.m_header.Dump(this.log)
}

func (this *Slookup_i) Diag_dump_entry(entry *slookup_i_lib_entry.Slookup_i_entry) {
	/* dump this entry and value */

	fmt.Println("entry: ", entry.Dump(false))

}

func (this *Slookup_i) Diag_dump_block(block_num uint32) {

	var ret tools.Ret
	var entry *slookup_i_lib_entry.Slookup_i_entry
	ret, entry = this.Lookup_entry_load(block_num)
	if ret == nil {
		this.Diag_dump_entry(entry)
	}
}

// func (this *Slookup_i) diag_dump_one(lp uint32) {

// 	var ret, nresp = this.Node_load(lp)
// 	if ret != nil {
// 		fmt.Println(ret)
// 		return
// 	}

// 	var n stree_v_node.Stree_node = *nresp
// 	fmt.Println("node pos:       ", lp)
// 	fmt.Println("parent:         ", n.Get_parent())
// 	fmt.Println("left child:     ", n.Get_left_child())
// 	fmt.Println("right child:    ", n.Get_right_child())
// 	fmt.Println("key:            ", n.Get_key())
// 	/* print beginning and end of data */
// 	if len(n.Get_value()) < 8 {
// 		fmt.Println("value:        ", tools.Dump(n.Get_value()))
// 	} else {
// 		var bout []byte // = make([]byte, 1)
// 		bout = append(bout, n.Get_value()[0:8]...)
// 		bout = append(bout, n.Get_value()[(len(n.Get_value())-8):]...)
// 		fmt.Println("value:        ", tools.Dump(bout))
// 	}

// 	fmt.Print("offspring:      ")
// 	if n.Is_offspring() {
// 		fmt.Println("none")
// 	} else {
// 		var rp uint32
// 		for rp = 0; rp < this.m_offspring_per_node; rp++ {

// 			var ret, offspring_resp = n.Get_offspring_pos(rp)
// 			if ret != nil {
// 				fmt.Println(ret)
// 				return
// 			}
// 			fmt.Print("", *offspring_resp, " ")
// 		}
// 		fmt.Println()
// 	}
// }

func (this *Slookup_i) Wipe() tools.Ret {
	return this.m_storage.Wipe()
}

func (this *Slookup_i) Dispose() tools.Ret {
	this.Shutdown()
	return this.m_storage.Dispose()
}
