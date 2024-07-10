package onebrc

import java.util.concurrent.ConcurrentHashMap
import java.io.{BufferedReader, File, FileInputStream, FileReader}
import java.nio.MappedByteBuffer
import java.nio.charset.StandardCharsets
import java.nio.file.{Path, StandardOpenOption}
import java.nio.channels.FileChannel
import scala.concurrent.{Await, Future, ExecutionContext}
import scala.concurrent.duration.Duration
import scala.concurrent.ExecutionContext.Implicits.global
import scala.collection.mutable.{LongMap, HashMap}

final case class Aggregate(var count: Int, var sum: Long, var min: Int, var max: Int):
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

  def toString(cityName: String): String =
    s"$cityName=${this.min / 10f}/${this.sum / this.count / 10f}/${this.max / 10f}"

object Aggregate:
  def fromReading(reading: Int): Aggregate =
    Aggregate(1, reading, reading, reading)

final val cityNames = new HashMap[Long, Array[Byte]](413, 0.3)

inline def parseLine(buffer: MappedByteBuffer, map: LongMap[Aggregate]): Unit =
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

  map.getOrNull(cityHash) match
    case null =>
      cityNames.put(cityHash, cityName)
      map.put(cityHash, Aggregate.fromReading(temperature))
    case item => item.addReading(temperature)

inline def parseLoop(buffer: MappedByteBuffer, bufferSize: Long): LongMap[Aggregate] =
  val localMap = LongMap[Aggregate]()
  while buffer.position() < bufferSize do
    parseLine(buffer, localMap)
  localMap

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
  val finalResults = LongMap[Aggregate]()
  val program = Future.sequence(tasks).map(_.foreach(_.foreach(tpl =>
      finalResults.getOrNull(tpl._1) match
        case null => finalResults.put(tpl._1, tpl._2)
        case agg =>  agg.merge(tpl._2)
    )
  ))
  Await.result(program, Duration.Inf)
  val resultsStr = StringBuilder("{")
  finalResults.toArray.map(tpl =>
    tpl._2.toString(new String(cityNames(tpl._1), StandardCharsets.UTF_8))
  ).sortInPlace.foreach(s => 
    resultsStr ++= s
    resultsStr ++= ", "
  )
  resultsStr += '}'
  println(resultsStr.toString)

  // println(s"Paris: ${state(hash("Paris", 5))}")
  // println(finalResults.size)
  // assert(finalResults.values.map(_.count).sum == 1_000_000_000)
  // assert(finalResults.size == 413)
