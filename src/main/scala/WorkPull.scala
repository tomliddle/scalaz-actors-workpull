import java.util.concurrent.Executors

import WorkPull._
import org.slf4j.LoggerFactory

import scala.collection.immutable.Queue
import scalaz.concurrent._


object WorkPull {
  implicit val pool = Executors.newFixedThreadPool(5)
  implicit val s = Strategy.Executor

	case class RequestWork(w: Worker) extends WorkerMessage(w)
	case class WorkFailed(w: Worker, url: String) extends WorkerMessage(w)
  case class Result(w: Worker, id: String, result: String) extends WorkerMessage(w)

	case class WorkAvailable() extends ParentMessage
	case class Work(work: String) extends ParentMessage

	abstract class ParentMessage()
	abstract class WorkerMessage(w: Worker)

	def main(args: Array[String]): Unit = {


		val wc = new WorkPull(5)

	}
}

/**
	* Main actor which stores urls that have been processed and the queue of urls to process
 *
	* @param noOfWorkers no of actors processing urls
	*/
class WorkPull(noOfWorkers: Int, queue: Queue[String]) {

	private val log = LoggerFactory.getLogger(getClass)
	private val workers  = (1 to noOfWorkers).map(_ => new Worker(parent))

  @volatile private var result = Map[String, String]()
  @volatile private var _queue = queue
  @volatile private var active = 0

	val parent: Actor[WorkerMessage] = Actor.actor{(m: WorkerMessage) => m match {

    case Result(_, id, res) =>
      active = active - 1

      log.debug(s"active count is now $active")

      result = result + (id -> res)

      println(s"${res}\n")

    case w: WorkFailed =>
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
}


