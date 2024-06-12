package onebrc

import java.io.{BufferedReader, File, FileReader}
import scala.concurrent.ExecutionContext
import scala.collection.mutable.LongMap

final case class Aggregate(var count: Int, var sum: Double, var min: Float, var max: Float):
  def addReading(reading: Float): Unit =
    this.count += 1
    this.sum += reading
    this.min = math.min(reading, this.min)
    this.max = math.max(reading, this.max)

object Aggregate:
  def fromReading(reading: Float): Aggregate =
    Aggregate(1, reading, reading, reading)

final val state: LongMap[Aggregate] = new LongMap(10000)

// This hashing function assumes we don't need to consume more than 9 characters
inline def hash(line: String, len: Int): Long =
  val iterations = math.min(9, len)
  var hash = line(0).toLong - 32
  var i = 1
  while i < iterations do {
    hash |= (line(i) - 32).toLong << i * 7
    i += 1
  }
  hash

inline def process(line: String): Unit =
  val separatorIdx = line.indexOf(';')
  val cityHash = hash(line, separatorIdx)
  val reading = line.slice(separatorIdx + 1, line.size).toFloat
  state.getOrNull(cityHash) match
    case null => state.update(cityHash, Aggregate.fromReading(reading))
    case item => item.addReading(reading)

@main def run: Unit =
  val file = new File("measurements.txt")
  val fileReader = new FileReader(file)
  val br = new BufferedReader(fileReader)
  Iterator.continually(br.readLine()).takeWhile(_ != null).foreach(process)
  // assert(state.values.map(_.count).sum == 1_000_000_000)
  // assert(state.size == 413)
