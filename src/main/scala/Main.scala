package onebrc

import java.nio.MappedByteBuffer
import java.nio.charset.StandardCharsets
import java.nio.file.{Path, StandardOpenOption}
import java.nio.channels.FileChannel
import scala.collection.mutable.LongMap
import scala.concurrent.{Await, Future}
import scala.concurrent.duration.Duration
import scala.concurrent.ExecutionContext.Implicits.global

final case class Aggregate(var count: Int, var sum: Int, var min: Int, var max: Int):
  def addReading(reading: Int): Unit =
    this.count += 1
    this.sum += reading
    this.min = if reading < this.min then reading else this.min
    this.max = if reading > this.max then reading else this.max
  
  def merge(other: Aggregate): Unit =
    this.count += other.count
    this.sum += other.sum
    this.min = if other.min < min then other.min else this.min
    this.max = if other.max > max then other.max else this.max

  def toStringWithCityName(cityName: String): String =
    s"$cityName=${this.min / 10f}/${this.sum / this.count / 10f}/${this.max / 10f}"

object Aggregate:
  def fromReading(reading: Int): Aggregate =
    Aggregate(1, reading, reading, reading)

final val cityNames = new LongMap[Array[Byte]](1 << 10)

inline def parseLine(buffer: MappedByteBuffer, localAggregates: LongMap[Aggregate]): Unit =
  var lastByte = buffer.get()
  var cityHash = 0l
  var i = 0
  val cityName = new Array[Byte](1 << 5)
  while lastByte != ';' do
    // This assumes we never need more than 9 characters in our hash,
    // because only 9*7 bits will fit in a Long
    cityHash |= lastByte.toLong << i * 7
    cityName(i) = lastByte
    i += 1
    lastByte = buffer.get()

  // Temperatures always have exactly one decimal digit. Therefore, we can parse them manually into
  // integers. 22.3 becomes 223, -4.1 becomes -41. We only need to /10 at the end.
  var isNegative = false
  lastByte = buffer.get()
  if lastByte == '-' then
    isNegative = true
    lastByte = buffer.get()
  var temperature = lastByte - 48
  lastByte = buffer.get()
  while lastByte != '\n' do
    if lastByte != '.' then
      temperature = (temperature * 10) + (lastByte - 48)
    lastByte = buffer.get()

  if isNegative then
    temperature = -temperature

  localAggregates.getOrNull(cityHash) match
    case null =>
      cityNames.put(cityHash, cityName)
      localAggregates.put(cityHash, Aggregate.fromReading(temperature))
    case item => item.addReading(temperature)

inline def parseLoop(buffer: MappedByteBuffer, bufferSize: Long): LongMap[Aggregate] =
  val localMap = new LongMap[Aggregate](1 << 10)
  while buffer.position() < bufferSize do
    parseLine(buffer, localMap)
  localMap

// Skip overflow from the previous chunk's last entry
inline def cleanChunk(buffer: MappedByteBuffer, beginning: Long): Unit =
  var lastByte = buffer.get()
  while lastByte != '\n' do
    lastByte = buffer.get()

final val cpuCores = Runtime.getRuntime().availableProcessors()

inline def processAndSort(): StringBuilder =
  val path = Path.of("measurements.txt");
  val channel = FileChannel.open(path, StandardOpenOption.READ)
  val fileSize = channel.size()
  val chunkSize = fileSize / cpuCores + 1
  val finalResults = new LongMap[Aggregate](1 << 9)
  val tasks = Future.traverse(0 until cpuCores)(n =>
    Future {
      val beginning = chunkSize * n
      val end = math.min(fileSize, beginning + chunkSize)
      val size = end - beginning
      val sizeWithSafeBuffer = if end == fileSize then size else size + 31
      val buffer = channel.map(
        FileChannel.MapMode.READ_ONLY,
        if n != 0 then beginning - 1 else 0,
        sizeWithSafeBuffer + 1,
      )
      // If this is the first chunk, we know that it begins at the beginning of a line
      if n != 0 then cleanChunk(buffer, beginning)
      parseLoop(buffer, size)
    }
  ).map(_.foreach(_.foreach(tpl =>
    finalResults.getOrNull(tpl._1) match
      case null => finalResults.put(tpl._1, tpl._2)
      case agg  => agg.merge(tpl._2)
  )))
  Await.result(tasks, Duration.Inf)
  val resultsStr = new StringBuilder(1 << 14, "{")
  finalResults
    .map(tpl =>
      tpl._2.toStringWithCityName(
        new String(cityNames(tpl._1), StandardCharsets.UTF_8)
      )
    )
    .toArray
    .sortInPlace
    .foreach(s => 
      resultsStr.append(s)
      resultsStr.append(", ")
    )
  resultsStr.replace(resultsStr.size - 2, resultsStr.size, "}")
  resultsStr

@main def run: Unit =
  println(processAndSort())

  // println(finalResults.size)
  // assert(finalResults.values.map(_.count).sum == 1_000_000_000)
  // assert(finalResults.size == 413)
