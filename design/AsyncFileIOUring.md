# AsyncFileIOUring
**Code:** `fdbrpc/AsyncFileIOUring.actor.h`

**Original PR**: https://github.com/apple/foundationdb/pull/4489/files

This is a working in progress IAsyncFile implementation using io_uring. 

## Knobs Added:
 - bool ENABLE_IO_URING;
	 - Use AsyncFileIOUring instead of AsyncFileKAIO
 - bool IO_URING_DIRECT_SUBMIT;
	 - Direct submit to the ring instead of queuing and batching
	 - Reduces the number of `io_uring_submit()` calls, which might be beneficial when used in conjunction with the `IO_URING_POLL` knob.
 - bool IO_URING_POLL;
	 - Enable io_uring kernel polling mode
	 - Not working!


## io_uring kernel polling mode
More documentation: https://unixism.net/loti/tutorial/sq_poll.html

Kernel thread polling mode is not fully implemented yet. To use kernel thread polling mode, the program needs to first register a list of files with `io_uring_register_files()`, then use the index instead of the file descriptor. However, the index is currently hard-coded only for some testing and is not ready for general use.


What needs to be done: 
- After opening a file, register the file descriptor with `io_uring_register_files()`. 
- Use the index of the registered file when performing I/O.
- Unregister the file with `io_uring_unregister_files()` when the file is closed. 


## How it works
The layout of this file is very similar to `AsyncFileKAIO`, but with io_uring function calls instead of aio. One notable difference is that io_uring has support for the fsync operation, and that's what `AsyncFileIOUring` uses for flushing data to disk, while `AsyncFileKAIO` uses `AsyncFileEIO::async_fdatasync`. 

It also has support for a direct submit mode controlled by the knob `IO_URING_DIRECT_SUBMIT`, which skips the op queue and submits work directly to the ring, this mode might be beneficial when using the kernel polling mode. 

### Code path for a write/read/sync operation:
**Submission side:**
Direct Submit:
 - User calls `AsyncFileIOUring::write` 
 - A call to `io_uring_get_sqe` is made to obtain a submission queue entry
 - sqe is populated with file descriptor, offset, data, etc. 
 - `io_uring_submit()` is called to submit the sqe to the kernel

Using op queue:
 - User calls `AsyncFileIOUring::write` 
 - Information about this operation gets enqueued into the op queue
 -  `AsyncFileIOUring::launch` function is called periodically from the `Net2::run` loop
 - All the operations in the op queue is converted into sqes
 - `io_uring_submit()` is called to submit the sqe to the kernel

**Completion side:** 
 - During initialization, `AsyncFileIOUring` registers an eventfd and starts the `poll_batch` loop to wait for events
 - Once eventfd read returns, we know that there are cqes in the ring, then a call to `io_uring_peek_batch_cqe` is made to retrieve the cqes
 - The promise associated with each cqe is resolved 
