package workpull

import org.slf4j.LoggerFactory
import workpull.WorkPull.{ParentMessage, Work, WorkAvailable, _}
import workpull.Worker._

import scala.concurrent.ExecutionContext
import scala.util.{Failure, Success, Try}
import scalaz.concurrent.Actor



object Worker {
  abstract class WorkerMessage(w: AbstractWorker)
  trait AbstractWorker {
    val workerActor: Actor[ParentMessage]
  }

  case class RequestWork(w: AbstractWorker) extends WorkerMessage(w)
  case class WorkFailed[T](w: AbstractWorker, url: T) extends WorkerMessage(w)
  case class Result[T, U](w: AbstractWorker, id: T, result: U) extends WorkerMessage(w)
}



/**
  * Fetches the resource from the url given in the work message. Sends a message to the parent with the parsed links
  *
  */
class Worker[T, U](parent: Actor[WorkerMessage], task: (T => U))(implicit val ec: ExecutionContext) extends AbstractWorker {


  private val log = LoggerFactory.getLogger(getClass)

  parent ! RequestWork(this)


  override val workerActor: Actor[ParentMessage] = Actor.actor {
    (p: ParentMessage) => p match {
      case w: WorkAvailable =>
        parent ! RequestWork(this)

      case Work(key: T) =>
        Try(task(key)) match {
          // A successful retrieval, notify the master
          case Success(w) =>
            parent ! Result(this, key, w)

          // A failure notify the master also so we can count down the requests
          case Failure(e) =>
            parent ! WorkFailed(this, key)

        }
        parent ! RequestWork(this)
    }
  }
}

