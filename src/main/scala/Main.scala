import java.io.{BufferedReader, File, FileReader}
import scala.concurrent.ExecutionContext
import scala.collection.mutable.LongMap
import scala.util.hashing.MurmurHash3 

final case class Aggregate(var count: Int, var sum: Double, var min: Float, var max: Float):
  def addReading(reading: Float): Unit =
    this.count += 1
    this.sum += reading
    this.min = math.min(reading, this.min)
    this.max = math.max(reading, this.max)

object Aggregate:
  def fromReading(reading: Float): Aggregate =
    Aggregate(1, reading, reading, reading)

val state: LongMap[Aggregate] = new LongMap(413)

inline def process(line: String): Unit =
  val separatorIdx = line.indexOf(";")
  val cityHash = line.slice(0, separatorIdx).hashCode()
  val reading = line.slice(separatorIdx + 1, line.size).toFloat
  state.getOrNull(cityHash) match {
    case null => state.update(cityHash, Aggregate.fromReading(reading))
    case item => item.addReading(reading)
  }

@main def run: Unit =
  val file = new File("measurements.txt")
  val fileReader = new FileReader(file)
  val br = new BufferedReader(fileReader)
  Iterator.continually(br.readLine()).takeWhile(_ != null).foreach(process)
  println(state.size)
