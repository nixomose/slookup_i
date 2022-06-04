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
	 marker and a matching end marker with the same id.
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
