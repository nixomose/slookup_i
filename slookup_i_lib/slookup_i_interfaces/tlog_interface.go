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

	Write_block(block_num uint32, n *[]byte) tools.Ret

	End_transaction() tools.Ret

	// these are owned by slookup_i, not the backing store.
	//Get_first_transaction_log_position() (tools.Ret, uint32)

	// Get_first_data_block_position() (tools.Ret, uint32)

	// Get_free_position() (tools.Ret, uint32)
}
