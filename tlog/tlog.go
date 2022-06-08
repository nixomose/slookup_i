package tlog

/* this is the implementation of the transaction log.

   you start a transaction
	 you write stuff to it,
	 you read stuff from it
	 you end/apply it

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
