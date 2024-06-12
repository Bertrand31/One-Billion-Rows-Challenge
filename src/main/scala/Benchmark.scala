package benchmarks

import java.util.concurrent.TimeUnit
import org.openjdk.jmh.annotations._
import org.openjdk.jmh.infra.Blackhole

object States {

  @State(Scope.Thread)
  class MyState {
    val line = "Paris;11.2"
  }
}

@BenchmarkMode(Array(Mode.Throughput))
@Measurement(iterations = 5, timeUnit = TimeUnit.SECONDS, time = 1)
@Warmup(iterations = 5, timeUnit = TimeUnit.SECONDS, time = 1)
@Fork(value = 2, jvmArgsAppend = Array())
@Threads(value = 1)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
class Benchmarks {

  @Benchmark
  def customHashing(state: States.MyState, blackhole: Blackhole): Unit =
    // state.line.toArray()
    val hash = onebrc.hash(state.line, 5)
    blackhole.consume(hash)

  @Benchmark
  def nativeHashing(state: States.MyState, blackhole: Blackhole): Unit =
    val hash = state.line.slice(0, 5).hashCode()
    blackhole.consume(hash)
}
