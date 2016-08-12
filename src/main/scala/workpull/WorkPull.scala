package workpull

import java.util.concurrent.Executors
import org.slf4j.LoggerFactory
import workpull.WorkPull.{Result, Work, _}
import scala.collection.immutable.Queue
import scala.concurrent.ExecutionContext
import scalaz.concurrent._


object WorkPull {
  implicit val pool = Executors.newFixedThreadPool(5)
  implicit val s = Strategy.Executor
  implicit val ec = ExecutionContext.fromExecutor(pool)

	case class RequestWork(w: Worker) extends WorkerMessage(w)
	case class WorkFailed(w: Worker, url: String) extends WorkerMessage(w)
  case class Result(w: Worker, id: String, result: String) extends WorkerMessage(w)
  abstract class WorkerMessage(w: Worker)

	case class WorkAvailable() extends ParentMessage
	case class Work(work: String) extends ParentMessage
	abstract class ParentMessage()


	def main(args: Array[String]): Unit = {
    val list = (0 to 10000).map(x => s"number $x")
		val wc = new WorkPull(100, Queue[String](list: _*), (String) => "result").run()
	}
}

/**
	* Main actor which stores urls that have been processed and the queue of urls to process
 *
	* @param noOfWorkers no of actors processing urls
	*/
class WorkPull(noOfWorkers: Int, queue: Queue[String], task: (String => String)) {

	private val log = LoggerFactory.getLogger(getClass)

  @volatile private var result = Map[String, String]()
  @volatile private var _queue = queue
  @volatile private var active = 0

	val parent: Actor[WorkerMessage] = Actor.actor {
    (m: WorkerMessage) => m match {

      case Result(_, id, res) =>
        active = active - 1

        log.debug(s"active count is now $active $res")

        result = result + (id -> res)

        checkFinished()

      case w: WorkFailed =>
        log.debug(s"URL failed to be parsed: $w")
        active = active - 1
        checkFinished()

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

  private def checkFinished(): Unit = {
    if (active == 0) {
      log.debug(result.values.zipWithIndex.mkString(" \n"))
      pool.shutdown()
    }
  }

  def run(): Unit = (1 to noOfWorkers).map(_ => new Worker(parent, task))
}


