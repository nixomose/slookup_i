// SPDX-License-Identifier: LGPL-2.1
// Copyright (C) 2021-2022 stu mark

package stree_v_lib

import "github.com/nixomose/nixomosegotools/tools"

type Stree_v_backing_store_interface interface {

	/* 6/3/2022 the stree interface is tailored to the stree implementation a bit
	   so we can't use it exactly for slookup, so we copied most of it here
		 and we'll just have to have a different implementation and caller
		 than we did for stree.
		 too bad. it would have been cool if the slookup matched stree enough
		 that we could reuse the interface and then it would have just worked. */

	Init() tools.Ret

	Is_backing_store_uninitialized() (tools.Ret, bool)

	Startup(force bool) tools.Ret

	Shutdown() tools.Ret

	Load_block_data(block_num uint32) (tools.Ret, *[]byte)

	Load_block_header(block_num uint32, len uint32) (tools.Ret, *[]byte)

	Store_block_header(block_num uint32, n *[]byte) tools.Ret
	Store_block_data(block_num uint32, n *[]byte) tools.Ret

	Get_root_node() (tools.Ret, uint32)

	Set_root_node(block_num uint32) tools.Ret

	Get_free_position() (ret tools.Ret, resp uint32)

	Get_total_blocks() (tools.Ret, uint32)

	Allocate(amount uint32) (tools.Ret, []uint32)

	Deallocate() tools.Ret

	Wipe() tools.Ret // zero out the first block so as to make it inittable again

	Dispose() tools.Ret
}

type Stree_v_backing_store_internal_interface interface {
	Init() tools.Ret

	Startup() tools.Ret

	Shutdown() tools.Ret

	Load(block_num uint32) (tools.Ret, *[]byte)

	Load_limit(block_num uint32, len uint32) (tools.Ret, *[]byte)

	Store(block_num uint32, n *[]byte) tools.Ret

	Get_root_node() (tools.Ret, uint32)

	Set_root_node(block_num uint32) tools.Ret

	Get_free_position() (ret tools.Ret, resp uint32)

	Get_total_blocks() (tools.Ret, uint32)

	Allocate(amount uint32) (tools.Ret, []uint32)

	Deallocate() tools.Ret

	Dispose() tools.Ret

	Print(*tools.Nixomosetools_logger)

	Get_logger() *tools.Nixomosetools_logger

	Diag_dump()
}
