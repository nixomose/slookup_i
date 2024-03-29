/* Package tlog has a comment */
package tlog

import (
	"github.com/nixomose/nixomosegotools/tools"
	slookup_i_lib_interfaces "github.com/nixomose/slookup_i/slookup_i_lib/slookup_i_interfaces"
)

/* this is the implementation of the transaction log.

   you start a transaction
	 you write stuff to it,
	 you read stuff from it
	 you mark it commit or not
	 you end/apply it, if commit not set, we roll it back/don't write the finish transcation block in the tlog

	 everything in stored in memory and on disk, all operations
	 are done out of memory, except recovery if we are started up
	 and there exists a transaction log with something in it.

	 upon recovery we only apply whole transactions, those with a start
	 marker and a matching end marker with the same  id.
	 you can have multiple transactions piled on the log one after the other

	 there's an in memory hash that has a pointer to the latest copy of
	 every block so reads can go quickly without having to scan the whole log
	 for each read */

/* we can add read and write locks here   sync.RWMutex
    for at least pretending to allow some parallelism.
		this is going to be tied up with the calling library that has to lock
		around the larger transaction set too, I think.
		we can't have a thread doing a read on a half-applied complex
		transaction (like a delete). but still, we will do what we can. */

/* so the tlog will be handed a (real) back end that can actually read and write to
disk that transactions apply to.
reads and writes to the tlog write out the log and perform reads by looking first
in the transaction history cache, and passing through to the actual read layer
when it's not found. this is quite like the read cache and the other thing like that
that I made in zos. I have to go look up what that was. oh yes, the write back cache
it would queue writes locally and perform reads from it and store the write queue
to disk so if you stopped and restarted it would pick up where it left off.
actually the only difference between that write back cache and this is that
we're going to be more strict and correct about the writes in the tlog so you can survive a
crash, which I don't think the write back cache could, and items in the write back cache
will be idempotent, which the write back cache weren't either, it just worked because
we mainatined the order of all the writes.  I think it had a quick hash lookup too.
everything comes full circle. */

type Transaction_log struct {
	log *tools.Nixomosetools_logger

	started_transaction bool
	commit_transaction  bool
}

// verify that tlog implements tlog_interface
var _ slookup_i_lib_interfaces.Transaction_log_interface = &Transaction_log{}
var _ slookup_i_lib_interfaces.Transaction_log_interface = (*Transaction_log)(nil)

func New_transaction_log() Transaction_log {
	var t Transaction_log
	t.started_transaction = false
	t.commit_transaction = false
	return t
}

func (this *Transaction_log) Init() tools.Ret {
	// write out the transaction log header
	return nil
}

func (this *Transaction_log) Startup(force bool) tools.Ret { // assumes replay
	// read transaction log off disk
	return nil
}
func (this *Transaction_log) Shutdown() tools.Ret { // should not flush the last transaction if still in flight.
	return nil
}
func (this *Transaction_log) Start_transaction() tools.Ret {
	if this.started_transaction != false {
		return tools.Error(this.log, "Transaction has already been started.")
	}
	this.commit_transaction = false
	return nil
}

func (this *Transaction_log) Read_block(block_num uint32) (tools.Ret, *[]byte) {
	return tools.Error(this.log, "not implemented yet."), nil
}

func (this *Transaction_log) Read_block_range(block_num_start uint32, block_num_end uint32) (tools.Ret, *[]byte) {
	return tools.Error(this.log, "not implemented yet."), nil
}
func (this *Transaction_log) Read_block_list(block_list []uint32, block_list_length uint32) (tools.Ret, *[]byte) {
	return tools.Error(this.log, "not implemented yet."), nil
}
func (this *Transaction_log) Write_block(block_num uint32, n *[]byte) tools.Ret {
	return tools.Error(this.log, "not implemented yet.")
}
func (this *Transaction_log) Write_block_range(start_block uint32, end_block uint32, alldata *[]byte) tools.Ret {
	return tools.Error(this.log, "not implemented yet.")
}

func (this *Transaction_log) Write_block_list(block_list []uint32, block_list_length uint32, alldata *[]byte) tools.Ret {
	return tools.Error(this.log, "not implemented yet.")
}

func (this *Transaction_log) Set_commit() {
	this.commit_transaction = true
}

func (this *Transaction_log) End_transaction() tools.Ret {
	if this.commit_transaction == false {
		// rollback
	} else {
		// write footer onto transaction log data
	}
	return nil
	// return tools.Error(this.log, "not implemented yet.")
}
