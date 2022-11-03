// SPDX-License-Identifier: LGPL-2.1
// Copyright (C) 2021-2022 stu mark

// Package slookup_i_lib ... has a comment
package slookup_i_lib

import "github.com/nixomose/nixomosegotools/tools"

type Transaction_log_interface interface {

	/* 6/12/22 the transaction log interface just has the set for starting a
	transaction, writing, reading and committing. there is no rollback really.
	there is only replay, and replay will only play fully completed transactions. */

	Init() tools.Ret

	Startup(force bool) tools.Ret // assumes replay

	Shutdown() tools.Ret // should not flush the last transaction if still in flight.

	Start_transaction() tools.Ret

	Read_block(block_num uint32) (tools.Ret, *[]byte)

	Read_block_range(block_num_start uint32, block_num_end uint32) (tools.Ret, *[]byte)

	Read_block_list(block_list []uint32, block_list_length uint32) (tools.Ret, *[]byte)

	Write_block(block_num uint32, block_list_length uint32, n *[]byte) tools.Ret

	Write_block_range(start_block uint32, end_block uint32, alldata *[]byte) tools.Ret

	Write_block_list(block_list []uint32, alldata *[]byte) tools.Ret

	Set_commit()

	End_transaction() tools.Ret
}
