package benchmarks

import java.util.concurrent.TimeUnit
import org.openjdk.jmh.annotations._
import org.openjdk.jmh.infra.Blackhole

object States {

  @State(Scope.Thread)
  class MyState {
    val line1 = "Paris' TestTresLong;11.2"
    val line2 = "Paris;48.0"
  }
}

@BenchmarkMode(Array(Mode.Throughput))
@Measurement(iterations = 3, timeUnit = TimeUnit.SECONDS, time = 1)
@Warmup(iterations = 3, timeUnit = TimeUnit.SECONDS, time = 4)
@Fork(value = 2, jvmArgsAppend = Array())
@Threads(value = 1)
@OutputTimeUnit(TimeUnit.MICROSECONDS)
class Benchmarks {

  @Benchmark
  def customHashing(state: States.MyState, blackhole: Blackhole): Unit =
    val hash1 = onebrc.hash(state.line1, 20)
    blackhole.consume(hash1)
    val hash2 = onebrc.hash(state.line2, 5)
    blackhole.consume(hash2)

  @Benchmark
  def nativeHashing(state: States.MyState, blackhole: Blackhole): Unit =
    val hash1 = state.line1.slice(0, 20).hashCode()
    blackhole.consume(hash1)
    val hash2 = state.line2.slice(0, 5).hashCode()
    blackhole.consume(hash2)

  @Benchmark
  def customFloatParsing(state: States.MyState, blackhole: Blackhole): Unit =
    val temp1 = onebrc.fakeFloatParse(state.line1, 20 + 1)
    val temp2 = onebrc.fakeFloatParse(state.line2, 5 + 1)
    blackhole.consume(temp1)
    blackhole.consume(temp2)

  @Benchmark
  def nativeFloatParsing(state: States.MyState, blackhole: Blackhole): Unit =
    val temp1 = state.line1.slice(21, state.line1.size).toFloat
    val temp2 = state.line2.slice(6, state.line2.size).toFloat
    blackhole.consume(temp1)
    blackhole.consume(temp2)

  @Benchmark
  def processLine(state: States.MyState, blackhole: Blackhole): Unit =
    val unit1 = onebrc.process(state.line1)
    blackhole.consume(unit1)
    val unit2 = onebrc.process(state.line2)
    blackhole.consume(unit2)
}
