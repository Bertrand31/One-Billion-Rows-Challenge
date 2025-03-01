## The One Billion Row Challenge (1BRC)

### Overview

My stab at the [1brc challenge](https://github.com/gunnarmorling/1brc/blob/main/README.md).
The original challenge is in Java, but my submission uses Scala.

### Current state

The current solution runs in 2.42 seconds on an Intel i7-13700H.

### How-to

Build: `sbt 'show GraalVMNativeImage/packageBin'`

Profile: `hyperfine -w 1 --runs 3 ./target/graalvm-native-image/onebrc`

### Notes to self: toolbox

https://perf.wiki.kernel.org/index.php/Tutorial
perf stat -e branches,branch-misses,cache-references,cache-misses,cycles,instructions,idle-cycles-backend,idle-cycles-frontend,task-clock ./target/graalvm-native-image/onebrc
https://github.com/async-profiler/async-profiler
And of course VisualVM
