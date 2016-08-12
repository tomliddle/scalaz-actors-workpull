package workpull

import java.util.concurrent.Executors

import org.slf4j.LoggerFactory
import workpull.WorkPull.{Work, _}
import workpull.Worker.{RequestWork, Result, WorkFailed, WorkerMessage}
import scala.collection.immutable.Queue
import scala.concurrent.ExecutionContext
import scalaz.concurrent._


object WorkPull {
  implicit val pool = Executors.newFixedThreadPool(5)
  implicit val s = Strategy.Executor
  implicit val ec = ExecutionContext.fromExecutor(pool)

  abstract class ParentMessage()
	case class WorkAvailable() extends ParentMessage
	case class Work[T](work: T) extends ParentMessage

  private val log = LoggerFactory.getLogger(getClass)

  // OK so we should do some testing. With this setup there isn't much to test
	def main(args: Array[String]): Unit = {
    val list = (0 to 10000).map(x => s"number $x")
		val wp = new WorkPull[String, String](100, Queue[String](list: _*), (String) => "result").run()
	}
}

/**
	* Class that dishes out the work
  * Note it *has an* actor rather than being one.
  * The actor is has is immutable
  * As opposed to Akka which allows messages of Any => Unit, we can specify which messages are allowed at compile time :)
  *
 *
	* @param noOfWorkers no of actors processing urls
	*/
class WorkPull[T, U](noOfWorkers: Int, queue: Queue[T], task: (T => U)) {

  @volatile private var _results = Map[T, U]()
  @volatile private var _queue = queue
  @volatile private var active = 0

	val parent: Actor[WorkerMessage] = Actor.actor {
    (m: WorkerMessage) => m match {

      case Result(_, id: T, res: U) =>
        active = active - 1

        log.debug(s"active count is now $active $res")

        // Update the results
        _results = _results + (id -> res)

      case w: WorkFailed[T] =>
        log.debug(s"URL failed to be parsed: $w")
        active = active - 1

      // A worker has requested work. If we have any available on the queue, send them a url to process.
      case r: RequestWork =>
        _queue.dequeueOption.foreach {
          case ((head, tail)) =>
            // Dequeue the work - we will only attempt this once
            _queue = tail

            // Send the work to the worker actor
            r.w.workerActor ! Work(head)

            active = active + 1

            log.debug(s"parsing $head active count is now $active")
        }
    }
  }

  def leftToProcess: Int = _queue.length

  def results: Map[T, U] = _results

  def close(force: Boolean = false): Unit = {
    if (force || active == 0) {
      log.debug(_results.values.zipWithIndex.mkString(" \n"))
      pool.shutdown()
    }
  }

  def run(): WorkPull[T, U] = {
    (1 to noOfWorkers).map(_ => new Worker(parent, task))
    // Allow chaining
    this
  }
}


