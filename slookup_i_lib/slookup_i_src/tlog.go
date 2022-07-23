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
	return tools.Error(this.log, "not implemented yet")
}

func (this *Tlog) Startup(force bool) tools.Ret {
	// assumes replay (whatever that means)
	return tools.Error(this.log, "not implemented yet")
}

func (this *Tlog) Shutdown() tools.Ret {
	return tools.Error(this.log, "not implemented yet")
} // should not flush the last transaction if still in flight.

func (this *Tlog) Start_transaction() tools.Ret {
	return tools.Error(this.log, "not implemented yet")
}

func (this *Tlog) Read_block(block_num uint32) (tools.Ret, *[]byte) {
	/* read a single block synchronously */
	return tools.Error(this.log, "not implemented yet"), nil
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

	var rets = make(chan tools.Ret)
	var alldata_lock sync.Mutex
	var alldata *[]byte
	*alldata = make([]byte, (block_num_end-block_num_start+1)*this.m_data_block_size_in_bytes)

	for lp := block_num_start; lp < (block_num_end + 1); lp++ {
		var destposstart = lp * this.m_data_block_size_in_bytes
		var destposend = destposstart + this.m_data_block_size_in_bytes

		go this.read_into_buffer(rets, lp, destposstart, destposend, &alldata_lock, alldata)
	}

	// wait for them all to come back.
	var ret tools.Ret = nil
	for wait := 0; wait < int(block_num_end-block_num_start+1); wait++ {
		var ret2 = <-rets
		if ret2 != nil {
			ret = ret2
		}
	}
	// alldata should be filled correctly if all went well
	return ret, alldata
}

func (this *Tlog) Write_block(block_num uint32, n *[]byte) tools.Ret {
	return tools.Error(this.log, "not implemented yet")
}

func (this *Tlog) End_transaction() tools.Ret {
	return tools.Error(this.log, "not implemented yet")
}