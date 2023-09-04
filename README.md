**squerrel_queue** is an MPMC queue designed for transferring serialized objects of varying sizes. It is implemented without locks. You can find a usage example in test.cpp.

It only requires the C++11 standard for compilation.

Please note that this project is currently in the early "it works on my laptop" stage. Code reviews are welcome and highly appreciated.

This algorithm can suffer from the ABA problem. For the issue to occur, it is necessary but not sufficient for the metadata counter with a bit size of `sizeof(Atom)*8-log2(DataBufferSize)-1` to overflow twice while a thread is stale. For the `Atom=uint64_t`, this is mainly a theoretical concern. However, when using `Atom=uint8_t`, the issue can be easily reproduced. If you have any suggestions on how to address this, I would be delighted to hear from you.
