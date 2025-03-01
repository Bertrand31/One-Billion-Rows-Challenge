package benchmarks

import java.util.concurrent.TimeUnit
import org.openjdk.jmh.annotations._
import org.openjdk.jmh.infra.Blackhole

object States {

  @State(Scope.Thread)
  class MyState {
    val line1 = "Paris' TestTresLong;11.2"
    val line2 = "Paris;48.0"
    var number = 32.1
    var number2 = 45.0
    val byte = 'a'.toByte
    val formatString = "%s - %f - %f"
  }
}

@BenchmarkMode(Array(Mode.Throughput))
@Measurement(iterations = 3, timeUnit = TimeUnit.SECONDS, time = 1)
@Warmup(iterations = 2, timeUnit = TimeUnit.SECONDS, time = 2)
@Fork(value = 2, jvmArgsAppend = Array())
@Threads(value = 1)
@OutputTimeUnit(TimeUnit.MICROSECONDS)
class Benchmarks:

  // @Benchmark
  // def times10(state: States.MyState, blackhole: Blackhole): Unit =
  //   blackhole.consume(state.number * 10)

  // @Benchmark
  // def times10magic(state: States.MyState, blackhole: Blackhole): Unit =
  //   blackhole.consume((state.number << 1) + (state.number << 3))

  // @Benchmark
  // def compareByteWithInt(state: States.MyState, blackhole: Blackhole): Unit =
  //   blackhole.consume(state.number == 46)

  // @Benchmark
  // def compareByteWithChar(state: States.MyState, blackhole: Blackhole): Unit =
  //   blackhole.consume(state.number == '.')

  // @Benchmark
  // def compareByteWithByte(state: States.MyState, blackhole: Blackhole): Unit =
  //   blackhole.consume(state.number == '.'.toByte)

  // @Benchmark
  // def ifelsemin(state: States.MyState, blackhole: Blackhole): Unit =
  //   blackhole.consume(if state.number < state.number2 then state.number else state.number2)

  // @Benchmark
  // def mathmin(state: States.MyState, blackhole: Blackhole): Unit =
  //   blackhole.consume(math.min(state.number, state.number2))

  @Benchmark
  def interpolation(state: States.MyState, blackhole: Blackhole): Unit =
    val str = s"${state.line1} - ${state.number} - ${state.number2}"
    blackhole.consume(str)

  @Benchmark
  def formatInterpolation(state: States.MyState, blackhole: Blackhole): Unit =
    val str = String.format(state.formatString, state.line1, state.number, state.number2)
    blackhole.consume(str)

  @Benchmark
  def stringBuilderInterpolation(state: States.MyState, blackhole: Blackhole): Unit =
    val str = new StringBuilder(1 << 5)
    str ++= state.line1
    str ++= " - "
    str ++= state.number.toString
    str ++= " - "
    str ++= state.number2.toString
    blackhole.consume(str.toString)
