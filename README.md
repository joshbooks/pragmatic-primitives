# pragmatic-primitives
A cooperative multithreading library based on work by Trevor Brown et al.

[![GitHub Super-Linter](https://github.com/joshbooks/pragmatic-primitives/workflows/Lint%20Code%20Base/badge.svg)](https://github.com/marketplace/actions/super-linter)


Trevor Brown, Faith Ellen, and Eric Ruppert wrote a rather wonderful paper establishing extensions to the apparently 
well known and much celebrated LL and SC operations (I hadn't heard of them before but it turns out they're kind of a big deal).
It's an awesome paper and the code they wrote enabled a non-blocking tree implementation that beats the pants off
other concurrent trees.

I thought it would be neat to have a Kotlin coroutine based implementation, so I did that. In the process I saw
an opportunity to put a work stealing twist on the cooperative part of the cooperative multithreading that I think
could be kind of neat. Need to do some bench marking to see if the work stealing is worth the additional overhead
for the coordination.
