package workpull


import java.util.concurrent.Executors

import org.slf4j.LoggerFactory
import workpull.WorkPull.{ParentMessage, RequestWork, Result, Work, WorkAvailable, WorkFailed, WorkerMessage, _}

import scala.concurrent.{ExecutionContext, Future}
import scala.util.{Failure, Success, Try}
import scalaz.concurrent.{Actor, Strategy}

/**
  * Fetches the resource from the url given in the work message. Sends a message to the parent with the parsed links
  *
  */
class Worker(parent: Actor[WorkerMessage]) {
  implicit val pool = Executors.newFixedThreadPool(5)
  implicit val s = Strategy.Executor
  implicit val ec = ExecutionContext.fromExecutor(pool)

  private val log = LoggerFactory.getLogger(getClass)

  parent ! RequestWork(this)


  val receive: (ParentMessage => Unit) = {

    case w: WorkAvailable =>
      parent ! RequestWork(this)

    case Work(key: String) =>
      doWork(key).map {
        // A successful retrieval, notify the master
        case Success(w) =>
          parent ! Result(this, key, w)

        // A failure notify the master also so we can count down the requests
        case Failure(e) =>
          parent ! WorkFailed(this, key)

      }.andThen {
        case _ => parent ! RequestWork(this)
      }
  }

  /**
    * Fetches the url and parses the html for relevant elements
    *
    * @param key
    * @return Future Try of the page
    */
  def doWork(key: String): Future[Try[String]] = {
    Future {
     Try{
       "result"
     }
    }
  }


  val workerActor: Actor[ParentMessage] = Actor.actor { receive }
}

