package workpull

import org.slf4j.LoggerFactory
import workpull.WorkPull.{ParentMessage, Work, WorkAvailable, _}
import workpull.Worker._
import scala.concurrent.ExecutionContext
import scala.util.{Failure, Success, Try}
import scalaz.concurrent.Actor



object Worker {
  // A worker needs a reference to its parent
  trait AbstractWorker {
    val workerActor: Actor[ParentMessage]
  }

  // A worker can only send a worker message. The parent can only listen to worker messages.
  abstract class WorkerMessage(w: AbstractWorker)
  case class RequestWork(w: AbstractWorker) extends WorkerMessage(w)
  case class WorkFailed[T](w: AbstractWorker, url: T) extends WorkerMessage(w)
  case class Result[T, U](w: AbstractWorker, id: T, result: U) extends WorkerMessage(w)
}



/**
  * Does the work and returns the result. Simple(z)
  * Note there isn't much logic in here so we can test the task separately. No need to roll out akka test ;)
  *
  */
class Worker[T, U](parent: Actor[WorkerMessage], task: (T => U))(implicit val ec: ExecutionContext) extends AbstractWorker {

  private val log = LoggerFactory.getLogger(getClass)

  // Request the work once we start up
  parent ! RequestWork(this)


  override val workerActor: Actor[ParentMessage] = Actor.actor {
    (p: ParentMessage) => p match {
      // We have received a message that work is available. Request it
      case w: WorkAvailable =>
        parent ! RequestWork(this)

      // We have received some work, do it and return the results
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

