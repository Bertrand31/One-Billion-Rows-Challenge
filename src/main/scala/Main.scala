package onebrc

import java.util.concurrent.ConcurrentHashMap
import java.io.{BufferedReader, File, FileInputStream, FileReader}
import java.nio.MappedByteBuffer
import java.nio.file.{Path, StandardOpenOption}
import java.nio.channels.FileChannel
import scala.concurrent.{Await, Future, ExecutionContext}
import scala.concurrent.duration.Duration
import scala.concurrent.ExecutionContext.Implicits.global
import scala.collection.mutable.LongMap

final case class Aggregate(count: Int, sum: Long, min: Int, max: Int):
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

final val state = new ConcurrentHashMap[Long, Aggregate](413, 0.6, 20)

inline def parseLine(buffer: MappedByteBuffer): Unit =
  var lastByte = buffer.get()
  var cityHash = 0l
  var i = 0
  while lastByte != 59 do // 59 is ';'
    // This assumes we never need more than 9 characters in our hash,
    // because only 9*7 bits will fit in a Long
    // println(s"$lastByte, ${lastByte.toChar}, ${lastByte.toChar.toInt}, ${lastByte.toLong.toBinaryString}")
    cityHash |= lastByte.toLong << i * 7
    i += 1
    lastByte = buffer.get()

  var isNegative = false
  var temperature = 0
  lastByte = buffer.get()
  if (lastByte == '-') then
    isNegative = true
  else 
    temperature += lastByte
  while (lastByte != 10) do // 10 is '\n'
    lastByte = buffer.get()
    // 2³a + 2a = 10a
    temperature = temperature << 3 + temperature << 1 + lastByte - 48

  if isNegative then
    temperature = -temperature
  state.compute(cityHash, (_, v) =>
    v match
      case null => Aggregate.fromReading(temperature)
      case item => item.addReading(temperature)
  )

inline def parseLoop(buffer: MappedByteBuffer, bufferSize: Long): Unit =
  while buffer.position() < bufferSize do
    parseLine(buffer)

// Skip overflow from the previous chunk's last entry
inline def cleanChunk(buffer: MappedByteBuffer, beginning: Long): Unit =
  var lastByte = buffer.get()
  while lastByte != 10 do // 10 is '\n'
    lastByte = buffer.get()

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
        val isFirstChunk = beginning == 0
        val size = end - beginning
        val sizeWithSafeBuffer = if end == fileSize then size else size + 31
        val buffer = channel.map(
          FileChannel.MapMode.READ_ONLY,
          if isFirstChunk then 0 else beginning - 1,
          sizeWithSafeBuffer + 1,
        )
        // If beginning == 0, it means this is the first chunk, and we know that is
        // begins at the beginning of a line
        if !isFirstChunk then cleanChunk(buffer, beginning)
        parseLoop(buffer, size)
      }
  })
  Await.result(Future.sequence(tasks), Duration.Inf)

  // import scala.jdk.CollectionConverters.*
  // println(s"Paris: ${state(hash("Paris", 5))}")
  // println(state.mappingCount())
  // assert(state.elements().asScala.map(_.count).sum == 1_000_000_000)
  // assert(state.size == 413)
