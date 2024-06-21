## The One Billion Row Challenge (1BRC)

### Overview

My stab at the [1brc challenge](https://github.com/gunnarmorling/1brc/blob/main/README.md).
The original challenge is in Java, but my submission uses Scala.

### Current state

The current solution runs in around 60 seconds on an Intel i7-13700H.
It is rather simplistic, and a lot remains to be done (reducing allocations, getting rid of `readLine()`, introducing parallelism).
The only fun things currently are [the hash function](https://github.com/Bertrand31/One-Billion-Rows-Challenge/blob/master/src/main/scala/Main.scala#L23), which is about 660% faster than the native `hashCode()` and the parsing of the floating-point values, which are parsed "manually" as integers (to be converted back to floating-points numbers at the end of the processing).
