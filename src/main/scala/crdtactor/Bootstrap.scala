package crdtactor

import scala.concurrent.duration.Duration
import scala.concurrent.duration.DurationInt
import scala.concurrent.Await
import org.apache.pekko.actor.typed.scaladsl.adapter.*
import org.apache.pekko.actor.ActorSystem
import org.apache.pekko.util.Timeout
import org.apache.pekko.actor.typed.scaladsl.Behaviors

//import akka.pattern.ask //TESTING
// import akka.util.Timeout
// import scala.concurrent.duration._
//import java.io._ //TESTING

object Bootstrap {
  //implicit val timeout: Timeout = 5.seconds //TESTING

  // def writeToFile(filename: String, id: Int, state: ddata.LWWMap[String, Int]): Unit = {
  //   val writer = new PrintWriter(new FileOutputStream(new File("./" + filename), true))
  //   writer.write(s"CRDTActor-$id: CRDT state: $state\n")
  //   writer.close()
  // }

  // Startup the actors and execute the workload
  def apply(): Unit =
    val N_ACTORS = 3

    Utils.setLoggerLevel("DEBUG")

    val system = ActorSystem("CRDTActor")

    // Create the actors
    val actors = (0 until N_ACTORS).map { i =>
      val name = s"CRDTActor-$i"
      val actorRef = system.spawn (
        Behaviors.setup[CRDTActor.Command] { ctx => new CRDTActor(i, ctx) },
        name
      )
      i -> actorRef
    }.toMap

    // Write actor addresses into the global state
    actors.foreach((id, actorRef) => Utils.GLOBAL_STATE.put(id, actorRef))

    // Start the actors
    actors.foreach((_, actorRef) => actorRef ! CRDTActor.Start)


    // Sleep for a few seconds, then quit :)
    Thread.sleep(2500)

    // Force quit
    System.exit(0)
}
