package workpull

import org.slf4j.LoggerFactory
import workpull.WorkPull.{ParentMessage, RequestWork, Result, Work, WorkAvailable, WorkFailed, WorkerMessage, _}
import scala.concurrent.{ExecutionContext, Future}
import scala.util.{Failure, Success, Try}
import scalaz.concurrent.{Actor, Strategy}

/**
  * Fetches the resource from the url given in the work message. Sends a message to the parent with the parsed links
  *
  */
class Worker(parent: Actor[WorkerMessage], task: (String => String))(implicit val ec: ExecutionContext) {

  private val log = LoggerFactory.getLogger(getClass)

  parent ! RequestWork(this)


  val workerActor: Actor[ParentMessage] = Actor.actor {
    (p: ParentMessage) => p match {
      case w: WorkAvailable =>
        parent ! RequestWork(this)

      case Work(key: String) =>
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

