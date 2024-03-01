package crdtactor
import scala.concurrent.duration.Duration
import scala.concurrent.duration.DurationInt
import scala.concurrent.Await

import org.apache.pekko.actor.typed.scaladsl.adapter._
import org.apache.pekko.actor.ActorSystem
import org.apache.pekko.util.Timeout
import org.apache.pekko.actor.typed.scaladsl.Behaviors

class testClass extends munit.FunSuite:
    import CRDTActorV2.*
    // import akka.actor.testkit.typed.scaladsl.ActorTestKit
    // import akka.actor.typed.Behaviors
    //import org.apache.pekko.actor.typed.scaladsl.Behaviors
    test("sum of two integers") {
        val obtained = 2 + 2
        val expected = 4
        assertEquals(obtained, expected)
    }

    test("all even numbers") {
        val input: List[Int] = List(1, 2, 3, 4)
        val obtainedResults: List[Int] = input.map(_ * 2)
        // check that obtained values are all even numbers
        assert(obtainedResults.forall(x => x % 2 == 0))
    }

    test("CRDT actors equality") {

        val system = ActorSystem("CRDTActorV2")

        val actors = (0 until 3).map { i =>
            val name = s"CRDTActor-$i"
            val actorRef = system.spawn(
                Behaviors.setup[CRDTActorV2.Command] { ctx => new CRDTActorV2(i, ctx) },
                name
            )
            i -> actorRef
        }.toMap

        actors.foreach((id, actorRef) => Utils.GLOBAL_STATE.put(id, actorRef))
        // Start the actors
        actors.foreach((_, actorRef) => actorRef ! CRDTActorV2.Start)

        actors(0) ! CRDTActorV2.Put("amount", 125)

        Thread.sleep(500)

        assert(actors(0).state.crdt == actors(2).state.crdt)
    }
    

    // test("CRDT actors equality") {
    //     //create two actors as a for loop
    //     val testKit = ActorTestKit()
        
    //     val actor1 = spawn(Behaviors.setup[CRDTActorV2.Command] { ctx => new CRDTActorV2(0, ctx) }, "CRDTActor-0")
    //     val actor2 = spawn(Behaviors.setup[CRDTActorV2.Command] { ctx => new CRDTActorV2(1, ctx) }, "CRDTActor-1")

    //     //add both actors to the Utils.Global.State
    //     Utils.GLOBAL_STATE.put(0, actor1)
    //     Utils.GLOBAL_STATE.put(1, actor2)
    //     actor1 ! CRDTActorV2.Start
    //     actor2 ! CRDTActorV2.Start

    //     //do a put msg to actor 1 that sends the string "amount" and the value 125 as an int
    //     actor1 ! Put("amount", 125)

    //     Thread.sleep(500)

    //     assert(actor1.state.crdt == actor2.state.crdt)
    // }
    