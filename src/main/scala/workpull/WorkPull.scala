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

	def main(args: Array[String]): Unit = {
    val list = (0 to 10000).map(x => s"number $x")
		val wp = new WorkPull[String, String](100, Queue[String](list: _*), (String) => "result").run()

    //if (wp.active == 0)
    //  wp.close()
	}
}

/**
	* Main actor which stores urls that have been processed and the queue of urls to process
 *
	* @param noOfWorkers no of actors processing urls
	*/
class WorkPull[T, U](noOfWorkers: Int, queue: Queue[T], task: (T => U)) {

	private val log = LoggerFactory.getLogger(getClass)

  @volatile private var result = Map[T, U]()
  @volatile private var _queue = queue
  @volatile private var active = 0

	val parent: Actor[WorkerMessage] = Actor.actor {
    (m: WorkerMessage) => m match {

      case Result(_, id: T, res: U) =>
        active = active - 1

        log.debug(s"active count is now $active $res")

        result = result + (id -> res)

      case w: WorkFailed[T] =>
        log.debug(s"URL failed to be parsed: $w")
        active = active - 1

      // A worker has requested work. If we have any available on the queue, send them a url to process.
      case r: RequestWork =>
        _queue.dequeueOption.foreach {
          case ((head, tail)) =>
            _queue = tail

            r.w.workerActor ! Work(head)

            active = active + 1

            log.debug(s"parsing $head active count is now $active")
        }
    }
  }

  def leftToProcess = active

  def close(force: Boolean = false): Unit = {
    if (force || active == 0) {
      log.debug(result.values.zipWithIndex.mkString(" \n"))
      pool.shutdown()
    }
  }

  def run(): WorkPull[T, U] = {
    (1 to noOfWorkers).map(_ => new Worker(parent, task))
    this
  }
}


