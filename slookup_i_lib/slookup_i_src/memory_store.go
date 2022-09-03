// SPDX-License-Identifier: LGPL-2.1
// Copyright (C) 2021-2022 stu mark

package slookup_i_src

import (
	"github.com/nixomose/nixomosegotools/tools"
	slookup_i_lib "github.com/nixomose/slookup_i/slookup_i_lib/slookup_i_interfaces"
)

type Memory_store struct {
	log *tools.Nixomosetools_logger

	initted bool
	started bool

	storage map[uint32][]byte

	// cache these becaues they're used all the time.
	root_node     uint32
	free_position uint32 // location of first free array element (equal to array length + 1)
}

// verify that memory_store implements backing_store
var _ slookup_i_lib.Slookup_i_backing_store_interface = &Memory_store{}
var _ slookup_i_lib.Slookup_i_backing_store_interface = (*Memory_store)(nil)

func New_memory_store(l *tools.Nixomosetools_logger) *Memory_store {
	var store Memory_store
	store.log = l
	store.Init()
	return &store
}

// func (this *Memory_store) Load_limit(pos uint32, len uint32) (tools.Ret, *[]byte) {
// 	/* for testing, it's helpful to have this actually work the way the file one does.
// 	it's easy to just call node.Load, but we need to simulate the real thing by not
// 	including the data. */
// 	var ret, data = this.Load(pos)
// 	if ret != nil {
// 		return ret, nil
// 	}
// 	var limit_data = (*data)[0:len]
// 	return nil, &limit_data
// }

func (this *Memory_store) Load_block_data(pos uint32) (tools.Ret, *[]byte) {
	var val, ok = this.storage[pos]
	if ok == false {
		var r = make([]byte, 4096)
		if pos == 1 {
			this.log.Debug("loading empty block: ", pos)
		}
		return nil, &r
	}
	if pos == 1 {
		this.log.Debug("loading existing block: ", pos)
	}
	return nil, &val
}

func (this *Memory_store) Store_block_data(pos uint32, data *[]byte) tools.Ret {
	this.storage[pos] = *data
	if pos == 1 {
		this.log.Debug("storing block: ", pos, " with ", len(*data), " bytes of data")
	}
	return nil
}

func (this *Memory_store) Is_backing_store_uninitialized() (tools.Ret, bool) {
	var uninitted = !this.initted
	return nil, uninitted

}

func (this *Memory_store) Startup(force bool) tools.Ret {
	if this.initted == false {
		return tools.Error(this.log, "memory store hasn't been initted yet, can't start up.")
	}
	if this.started != false {
		return tools.Error(this.log, "memory store has already been starteed up, not starting again")
	}

	this.storage = make(map[uint32][]byte)
	this.root_node = 0
	this.free_position = 1 // zero is special value, so array allocation positions start at 1
	this.started = true
	/* zero is the location where we store our header inforation if we were storing on disk, this memory implementation
	doesn't do that, but if we were storing blocks on disk, this is what reserves block 0 for the file header. */
	return nil
}

func (this *Memory_store) Shutdown() tools.Ret {
	// we don't need to manually delete all the values, but we can
	if this.initted == false {
		return tools.Error(this.log, "memory store hasn't been initted yet, can't shut down.")
	}
	if this.started == false {
		return tools.Error(this.log, "memory store hasn't been started, can't be shut down")
	}

	for k := range this.storage {
		delete(this.storage, k)
	}
	return nil
}

func (this *Memory_store) Get_root_node() (tools.Ret, uint32) {
	this.log.Debug("get root node: ", this.root_node)
	return nil, this.root_node
}

func (this *Memory_store) Set_root_node(pos uint32) tools.Ret {
	this.log.Debug("set root node to ", pos)
	this.root_node = pos
	return nil
}

func (this *Memory_store) Init() tools.Ret {
	this.initted = true
	this.started = false
	return nil
}

func (this *Memory_store) Get_free_position() (tools.Ret, uint32) {
	this.log.Debug("get free position: ", this.free_position)
	return nil, this.free_position
}

func (this *Memory_store) Set_free_position(pos uint32) tools.Ret {
	this.log.Debug("set free position to: ", pos)
	this.free_position = pos
	return nil
}

func (this *Memory_store) Get_total_blocks() (tools.Ret, uint32) {
	// the memory implementation grows dynaymically so there's no total really.
	var ret uint32 = 1000000
	return nil, ret
}

// func (this *Memory_store) Allocate(amount uint32) (tools.Ret, []uint32) { /* allocate i blocks from free position and return an array of the positions allocated */
// 	var lp uint32
// 	var rvals []uint32 = make([]uint32, amount)
// 	for lp = 0; lp < amount; lp++ {
// 		rvals[lp] = this.free_position
// 		this.free_position++
// 	}
// 	this.log.Debug("allocating ", amount, " blocks, new free position is: ", this.free_position)
// 	return nil, rvals
// }

// func (this *Memory_store) Deallocate() tools.Ret {
// 	this.log.Debug("deallocating one block...")

// 	this.Set_free_position(this.free_position - 1)
// 	// clear the map entry
// 	delete(this.storage, this.free_position) // xxxz check this
// 	return nil
// }

func (this *Memory_store) Mark_end(free_position uint32) tools.Ret {
	return nil
}

func (this *Memory_store) Wipe() tools.Ret {
	return nil
}

func (this *Memory_store) Dispose() tools.Ret {
	return nil
}
