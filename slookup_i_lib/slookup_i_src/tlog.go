// SPDX-License-Identifier: LGPL-2.1
// Copyright (C) 2021-2022 stu mark

/* this is the implementation of the transaction log for slookup_i
   I suppose it could have been the write back log I wrote for zendemic
	 and while that would function for the purposes of making sure all
	 writes eventually completed, I wanted this to be more of a
	 'perform a multi-block atomic operation' kind of thing.
	 so that's what this is. */

// package name must match directory name
package slookup_i_src

import (
	"sync"

	"github.com/nixomose/nixomosegotools/tools"
	slookup_i_lib "github.com/nixomose/slookup_i/slookup_i_lib/slookup_i_interfaces"
	slookup_i_lib_interfaces "github.com/nixomose/slookup_i/slookup_i_lib/slookup_i_interfaces"
)

type Tlog struct {
	interface_lock sync.Mutex
	log            *tools.Nixomosetools_logger

	m_storage slookup_i_lib_interfaces.Slookup_i_backing_store_interface

	m_data_block_size_in_bytes uint32
	m_total_blocks             uint32
	m_commit                   bool
}

// verify that tlog implements the interface
var _ slookup_i_lib_interfaces.Transaction_log_interface = &Tlog{}
var _ slookup_i_lib_interfaces.Transaction_log_interface = (*Tlog)(nil)

func New_Tlog(l *tools.Nixomosetools_logger, storage slookup_i_lib.Slookup_i_backing_store_interface,
	data_block_size_in_bytes uint32, total_blocks uint32) *Tlog {

	/* the tlog is injected into the slookup_i, which is the thing that knows about the geometry of the backing store.
	so basically when it's created, the user passes all the information in, when it's starting up an existing store,
	it gets all the information from the header, so the very top level will have to read the header from the backing store
	raw, as in not through the transaction log, and parse out the header and create the slookup_i and the tlog with
	the parameters found therin. of andor. */

	var t Tlog
	t.log = l
	t.m_storage = storage
	t.m_data_block_size_in_bytes = data_block_size_in_bytes
	t.m_total_blocks = total_blocks

	return &t
}

func (this *Tlog) Get_logger() *tools.Nixomosetools_logger {
	return this.log
}

func (this *Tlog) Is_initialized() (tools.Ret, bool) {
	return tools.Error(this.log, "not implemented yet"), false
}
func (this *Tlog) Init() tools.Ret {
	// write out the transaction log header
	return nil
}

func (this *Tlog) replay(force bool) tools.Ret {
	// if there's anything unplayed in the transaction log, apply it and mark it done.
	return nil
}
func (this *Tlog) Startup(force bool) tools.Ret {
	// assumes replay (whatever that means)
	return this.replay(force)
}

func (this *Tlog) Shutdown() tools.Ret {
	return nil
} // should not flush the last transaction if still in flight.

func (this *Tlog) Start_transaction() tools.Ret {
	this.m_commit = false
	return nil
	// return tools.Error(this.log, "not implemented yet")
}

func (this *Tlog) Read_block(block_num uint32) (tools.Ret, *[]byte) {
	/* read a single block synchronously. if it's in the transaction log, return that,
	if not, go after the backing storage. */
	return this.m_storage.Load_block_data(block_num)
}

func (this *Tlog) read_into_buffer(rets chan<- tools.Ret, block_num uint32, destposstart uint32, destposend uint32,
	alldata_lock *sync.Mutex, alldata *[]byte) {
	var data *[]byte = nil
	var ret tools.Ret
	ret, data = this.Read_block(block_num)
	if ret == nil {
		// copy the data into the correct place in the alldata array
		alldata_lock.Lock()
		var copied = copy((*alldata)[destposstart:destposend], *data)
		if copied != len(*data) {
			rets <- tools.Error(this.log, "didn't get all the data from an entry block read, expected: ", data, " got: ", copied)
			alldata_lock.Unlock()
			return
		}
		alldata_lock.Unlock()
	}

	rets <- ret
}

func (this *Tlog) Read_block_range(block_num_start uint32, block_num_end uint32) (tools.Ret, *[]byte) {
	/* call read single block in parallel and send back the resulting filled in array of bytes from all those blocks.
	really we should probably read a range in series as one long read and read random sets of ranges in parallel.
	I guess we'll test and see which is faster. For stree reading a range of blocks bought no benefit, and
	actually for this it doesn't either, the only savings it gets you is reading the lookup table, the
	actual consecutive blocks are sprinkled all over the place, so that's why I'm doing this.
	and most of the time, the lookup table entry read will be one block anyway. */
	// end_block is not inclusive

	if block_num_end-block_num_start < 1 {
		return tools.Error(this.log, "invalid read block range request: ", block_num_start, " to ", block_num_end), nil
	}
	var rets = make(chan tools.Ret)
	var alldata_lock sync.Mutex
	var alldata []byte = make([]byte, (block_num_end-block_num_start)*this.m_data_block_size_in_bytes)

	var lp uint32
	for lp = 0; lp < block_num_end-block_num_start; lp++ {
		var destposstart = lp * this.m_data_block_size_in_bytes
		var destposend = destposstart + this.m_data_block_size_in_bytes

		go this.read_into_buffer(rets, lp, destposstart, destposend, &alldata_lock, &alldata)
	}

	// wait for them all to come back. xxxz change to wait group?
	var ret tools.Ret = nil
	for wait := 0; wait < int(block_num_end-block_num_start); wait++ {
		var ret2 = <-rets
		if ret2 != nil {
			ret = ret2
		}
	}
	// alldata should be filled correctly if all went well
	return ret, &alldata
}

func (this *Tlog) Read_block_list(block_list []uint32) (tools.Ret, *[]byte) {
	/* like above, but it gets a list of blocks. returns the data in the byte array */

	var rets = make(chan tools.Ret)
	var alldata_lock sync.Mutex
	var alldata *[]byte
	*alldata = make([]byte, uint32(len(block_list))*this.m_data_block_size_in_bytes)

	var lp uint32
	for lp = 0; lp < uint32(len(block_list)); lp++ {
		var destposstart = lp * this.m_data_block_size_in_bytes
		var destposend = destposstart + this.m_data_block_size_in_bytes

		go this.read_into_buffer(rets, lp, destposstart, destposend, &alldata_lock, alldata)
	}

	// wait for them all to come back.
	var ret tools.Ret = nil
	for wait := 0; wait < len(block_list); wait++ {
		var ret2 = <-rets
		if ret2 != nil {
			ret = ret2
		}
	}
	// alldata should be filled correctly if all went well
	return ret, alldata
}

func (this *Tlog) Write_block(block_num uint32, n *[]byte) tools.Ret {
	/* add this write block to the transaction log, or overwrite an existing entry
	for this block_num if it's already in the transaction log. */
	return this.m_storage.Store_block_data(block_num, n)
}

func (this *Tlog) write_from_buffer(rets chan<- tools.Ret, block_num uint32, destposstart uint32, destposend uint32,
	alldata_lock *sync.Mutex, alldata *[]byte) {
	var data []byte
	if alldata == nil {
		data = make([]byte, this.m_data_block_size_in_bytes)
	} else {
		data = (*alldata)[destposstart:destposend]
	}
	var ret = this.Write_block(block_num, &data)
	alldata_lock.Unlock()
	rets <- ret
}

func (this *Tlog) Write_block_range(block_num_start uint32, block_num_end uint32, alldata *[]byte) tools.Ret {
	/* call write single block in parallel getting the data from slices of alldata. */
	// end_block is not inclusive
	// if alldata is null, write zeros

	var rets = make(chan tools.Ret)
	var alldata_lock sync.Mutex

	for lp := block_num_start; lp < block_num_end; lp++ {
		var destposstart = lp * this.m_data_block_size_in_bytes
		var destposend = destposstart + this.m_data_block_size_in_bytes

		go this.write_from_buffer(rets, lp, destposstart, destposend, &alldata_lock, alldata)
	}

	// wait for them all to come back.
	var ret tools.Ret = nil
	for wait := 0; wait < int(block_num_end-block_num_start); wait++ {
		var ret2 = <-rets
		if ret2 != nil {
			ret = ret2
		}
	}
	return ret
}

func (this *Tlog) Write_block_list(block_list []uint32, alldata *[]byte) tools.Ret {
	/* same like above but for random blocks in the block_list
	call write single block in parallel getting the data from slices of alldata. */

	var rets = make(chan tools.Ret)
	var alldata_lock sync.Mutex

	for lp, block_num := range block_list {
		var destposstart = uint32(lp) * this.m_data_block_size_in_bytes
		var destposend = destposstart + this.m_data_block_size_in_bytes

		go this.write_from_buffer(rets, block_num, destposstart, destposend, &alldata_lock, alldata)
	}

	// wait for them all to come back.
	var ret tools.Ret = nil
	for wait := 0; wait < len(block_list); wait++ {
		var ret2 = <-rets
		if ret2 != nil {
			ret = ret2
		}
	}
	return ret
}

func (this *Tlog) Set_commit() {
	this.m_commit = true
}

func (this *Tlog) End_transaction() tools.Ret {
	return nil
	// return tools.Error(this.log, "not implemented yet")
}
