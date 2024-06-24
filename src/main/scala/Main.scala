package onebrc

import java.io.{BufferedReader, File, FileReader}
import scala.concurrent.ExecutionContext
import scala.concurrent.ExecutionContext.Implicits.global
import scala.collection.mutable.LongMap
import java.io.FileInputStream
import java.nio.channels.FileChannel
import java.nio.file.Path
import java.nio.file.StandardOpenOption
import java.nio.MappedByteBuffer
import scala.concurrent.Future
import scala.concurrent.Await
import scala.concurrent.duration.Duration
import java.util.concurrent.ConcurrentHashMap

final case class Aggregate(var count: Int, var sum: Long, var min: Int, var max: Int):
  def addReading(reading: Int): Aggregate =
    this.copy(
      count = count + 1,
      sum = sum + reading,
      min = if reading < this.min then reading else this.min,
      max = if reading > this.max then reading else this.max,
    )

  override def toString(): String =
    s"Avg ${this.sum / this.count / 10f} Max ${this.max / 10f} Min ${this.min / 10f}"

object Aggregate:
  def fromReading(reading: Int): Aggregate =
    Aggregate(1, reading, reading, reading)

final val state: ConcurrentHashMap[Long, Aggregate] = new ConcurrentHashMap(700, 0.75, 20)

inline def parseLoop(buffer: MappedByteBuffer): Unit =
  var lastByte: Byte = buffer.get()
  var cityHash: Long = 0l
  var i = 0
  // 59 is ';'
  while (i < 9 && lastByte != 59) do
    cityHash |= lastByte.toLong << i * 7
    i += 1
    lastByte = buffer.get()
  var isNegative = false
  var temperature = 0
  lastByte = buffer.get()
  if (lastByte == '-') {
    isNegative = true
  } else {
    temperature += lastByte
  }
  // 10 is '\n'
  while (lastByte != 10) do
    lastByte = buffer.get()
    val short = lastByte - 48
    if (short >= 0 && short < 10) {
      temperature *= 10
      temperature += short
    }
  if (isNegative) {
    temperature = -temperature
  }
  state.compute(cityHash, (_, v) =>
    v match
      case null => Aggregate.fromReading(temperature)
      case item => item.addReading(temperature)
  )

final val cpuCores = Runtime.getRuntime().availableProcessors()

@main def run: Unit =
  val path = Path.of("measurements.txt");
  val channel = FileChannel.open(path, StandardOpenOption.READ)
  val fileSize = channel.size()
  val chunkSize = fileSize / cpuCores
  val tasks = (1 until cpuCores).foldLeft(List((0l, chunkSize)))((acc, n) =>
    val end =
      if n == cpuCores - 1 then fileSize
      else acc.head._2 + chunkSize
    (acc.head._2, end) :: acc
  ).map({
    case (beginning, end) =>
      Future {
        val size = end - beginning
        val sizeWithSafeBuffer = if end == fileSize then size else size + 31
        val buffer = channel.map(
          FileChannel.MapMode.READ_ONLY,
          if beginning == 0 then beginning else beginning - 1,
          sizeWithSafeBuffer + 1,
        )
        var bufferPosition = 0
        // Skipping overflow from the previous chunk's last entry
        if beginning != 0 then
          var lastChar: Byte = buffer.get()
          // 10 is '\n'
          while lastChar != 10 do
            lastChar = buffer.get()

        while bufferPosition < size do
          parseLoop(buffer)
          bufferPosition = buffer.position()
      }
  })
  Await.result(Future.sequence(tasks), Duration.Inf)

  // import scala.jdk.CollectionConverters.*
  // println(s"Paris: ${state(hash("Paris", 5))}")
  // println(state.mappingCount())
  // assert(state.elements().asScala.map(_.count).sum == 1_000_000_000)
  // assert(state.size == 413)
