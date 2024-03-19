import cats.implicits.*
import cats.effect.*
import fs2.{Stream, text}
import fs2.io.file.{Files, Path}
import cats.effect.kernel.Ref
import cats.kernel.Monoid
import cats.kernel.Semigroup
import scala.concurrent.ExecutionContext
import java.util.concurrent.Executors
import cats.effect.std.Queue
import cats.effect.std.Mutex
import scala.collection.mutable.HashMap
import scala.collection.immutable.ArraySeq

final case class Datum(city: String, reading: Double)

final case class Aggregate(count: Int, sum: Double, min: Float, max: Float):
  def addReading(reading: Float): Aggregate =
    Aggregate(
      count = this.count + 1,
      sum = this.sum + reading,
      min = math.min(this.min, reading),
      max = math.max(this.max, reading),
    )

object Aggregate:
  def fromReading(reading: Float): Aggregate =
    Aggregate(1, reading, reading, reading)

object App extends IOApp.Simple:

  private val state: HashMap[String, Aggregate] = HashMap.empty

  def run: IO[Unit] =
    for {
      _ <- Files[IO]
        .readUtf8Lines(Path("measurements.txt"))
        .chunkN(100_000, true)
        .foreach(chunk =>
          chunk.foreach(
            _.split(";") match {
              case Array(city, readingStr) =>
                state.get(city) match {
                  case None => state.update(city, Aggregate.fromReading(readingStr.toFloat))
                  case Some(item) => state.update(city, item.addReading(readingStr.toFloat))
                }
              case _ =>
            }
          )
          IO.unit
        )
        .compile.drain
      _ <- IO.println(state)
    } yield ()
