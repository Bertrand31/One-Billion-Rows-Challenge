## The One Billion Row Challenge (1BRC)

### Overview

My stab at the [1brc challenge](https://github.com/gunnarmorling/1brc/blob/main/README.md).
The original challenge is in Java, but my submission uses Scala.

### Current state

The current solution runs in around 30 seconds on an Intel i7-13700H.
It is still a WIP, and parallelism has yet to be introduced. In its current state, the only thing that remains to be done before introducing parallelism is to find a solution for different threads to interact with the same keys of the LongMap simulatneously.
