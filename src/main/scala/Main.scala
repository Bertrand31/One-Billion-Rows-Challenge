package onebrc

import java.io.{BufferedReader, File, FileReader}
import scala.concurrent.ExecutionContext
import scala.collection.mutable.LongMap
import java.io.FileInputStream
import java.nio.channels.FileChannel
import java.nio.file.Path
import java.nio.file.StandardOpenOption
import java.nio.MappedByteBuffer

final case class Aggregate(var count: Int, var sum: Long, var min: Int, var max: Int):
  def addReading(reading: Int): Unit =
    this.count += 1
    this.sum += reading
    this.min = math.min(reading, this.min)
    this.max = math.max(reading, this.max)

  override def toString(): String =
    s"Avg ${this.sum / this.count / 10f} Max ${this.max / 10f} Min ${this.min / 10f}"

object Aggregate:
  def fromReading(reading: Int): Aggregate =
    Aggregate(1, reading, reading, reading)

final val state: LongMap[Aggregate] = new LongMap(700)

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
  state.getOrNull(cityHash) match
    case null => state.update(cityHash, Aggregate.fromReading(temperature))
    case item => item.addReading(temperature)

final val cpuCores = Runtime.getRuntime().availableProcessors()

@main def run: Unit =
  val path = Path.of("measurements.txt");
  val channel = FileChannel.open(path, StandardOpenOption.READ)
  val fileSize = channel.size()
  val chunkSize = fileSize / cpuCores
  (1 until cpuCores).foldLeft(List((0l, chunkSize)))((acc, n) =>
    val end =
      if n == cpuCores - 1 then fileSize
      else acc.head._2 + chunkSize
    (acc.head._2, end) :: acc
  ).foreach({
    case (beginning, end) =>
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
  })
  // println(s"Paris: ${state(hash("Paris", 5))}")
  // println(state.values.map(_.count).sum)
  // assert(state.values.map(_.count).sum == 1_000_000_000)
  // assert(state.size == 413)
