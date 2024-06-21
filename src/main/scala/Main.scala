package onebrc

import java.io.{BufferedReader, File, FileReader}
import scala.concurrent.ExecutionContext
import scala.collection.mutable.LongMap

final case class Aggregate(var count: Int, var sum: Long, var min: Int, var max: Int):
  def addReading(reading: Int): Unit =
    this.count += 1
    this.sum += reading
    this.min = math.min(reading, this.min)
    this.max = math.max(reading, this.max)

  def show: String =
    s"Avg ${this.sum / this.count / 10f} Max ${this.max / 10f} Min ${this.min / 10f}"

object Aggregate:
  def fromReading(reading: Int): Aggregate =
    Aggregate(1, reading, reading, reading)

final val state: LongMap[Aggregate] = new LongMap(10000)

final val NeededChars = 9

// This hashing function assumes we don't need to consume more than 9 characters.
// A character is 8 bits, but any character used in the data fits on 7 bits.
// 7 * 9 = 63 therfore 9 characters can fit in a single Long.
inline def hash(line: java.lang.String, len: Int): Long =
  var iterations = math.min(NeededChars, len) - 1
  var hash = line.charAt(iterations).toLong
  while iterations >= 0 do
    hash |= line.charAt(iterations).toLong << iterations * 7
    iterations -= 1
  hash

// Temperatures are all <100 and >-100, and always have exactly one decimal digit.
// Therefore, we can parse them manually into integers. 22.3 becomes 223, -4.1 becomes -41.
inline def fakeFloatParse(line: java.lang.String, startIdx: Int): Int =
  val isNegative = line.charAt(startIdx) == '-'
  val idx = startIdx + (if isNegative then 1 else 0)
  val parsedInt =
    (line.size - idx) match
      // 2.3
      case 3 =>
        (line.charAt(idx) - 48) * 10 +
        (line.charAt(idx + 2) - 48)
      // 12.3
      case 4 =>
        (line.charAt(idx) - 48) * 100 +
        (line.charAt(idx + 1) - 48) * 10 +
        (line.charAt(idx + 3) - 48)
  if isNegative then -parsedInt else parsedInt
  
inline def process(line: String): Unit =
  val separatorIdx = line.indexOf(';')
  val cityHash = hash(line, separatorIdx)
  val reading = fakeFloatParse(line, separatorIdx + 1)
  state.getOrNull(cityHash) match
    case null => state.update(cityHash, Aggregate.fromReading(reading))
    case item => item.addReading(reading)

@main def run: Unit =
  val file = new File("measurements.txt")
  val fileReader = new FileReader(file)
  val br = new BufferedReader(fileReader)
  Iterator.continually(br.readLine()).takeWhile(_ != null).foreach(process)
  println(s"Paris: ${state(hash("Paris", 5)).show}")
  // assert(state.values.map(_.count).sum == 1_000_000_000)
  // assert(state.size == 413)
