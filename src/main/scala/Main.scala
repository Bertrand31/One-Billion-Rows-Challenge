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

final case class Datum(city: String, aggregate: Aggregate)

object Datum:
  def fromString(str: String): Datum =
    str.split(";").toList match {
      case city :: readingStr :: Nil => Datum(city, Aggregate.fromReading(readingStr.toDouble))
      case _ => throw new Exception("kek")
    }

final case class Aggregate(count: Int, sum: Double, min: Double, max: Double):
  def addReading(reading: Double): Aggregate =
    Aggregate(
      count = this.count + 1,
      sum = this.sum + reading,
      min = math.min(this.min, reading),
      max = math.max(this.max, reading),
    )

object Aggregate:
  def fromReading(reading: Double): Aggregate =
    Aggregate(1, reading, reading, reading)

given aggregateMonoid: Monoid[Aggregate] = new Monoid[Aggregate] {
  def empty: Aggregate = Aggregate(1, 0, 0, 0)

  def combine(x: Aggregate, y: Aggregate): Aggregate =
    Aggregate(
      count = x.count + y.count,
      sum = x.sum + y.sum,
      min = math.min(x.min, y.min),
      max = math.max(x.min, y.min),
    )
}

object App extends IOApp.Simple:

  private val state: HashMap[String, Aggregate] = HashMap.empty

  def spawnConsumers(queues: ArraySeq[Queue[IO, Datum]]): IO[ArraySeq[FiberIO[Nothing]]] =
    queues.zipWithIndex.traverse({
      case (queue, index) =>
        (queue.tryTake.flatMap({
          case None => IO.cede
          case Some(item) =>
            val old = state.get(item.city).getOrElse(Monoid[Aggregate].empty)
            state.update(item.city, old |+| item.aggregate)
            IO.unit
        })).foreverM.start
    })

  val bucketsNb = Runtime.getRuntime().availableProcessors() - 1
  val queuesIO: IO[ArraySeq[Queue[IO, Datum]]] = 
      ArraySeq.fill(bucketsNb)(Queue.bounded[IO, Datum](10000)).sequence

  def run: IO[Unit] =
    for {
      queues <- queuesIO
      consumersHandles <- spawnConsumers(queues)
      _ <- Files[IO]
        .readUtf8Lines(Path("measurements-short.txt"))
        .filter(_.nonEmpty)
        .map(Datum.fromString)
        .zipWithIndex
        .foreach({
          case (datum, index) =>
            val bucket = math.abs(datum.city.hashCode()) % bucketsNb
            val queue = queues(bucket)
            queue.offer(datum)
        })
        .compile.drain
      _ <- IO.println(state)
    } yield ()
