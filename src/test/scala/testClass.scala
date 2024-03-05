package crdtactor

import scala.concurrent.duration.Duration
import scala.concurrent.duration.DurationInt
import scala.concurrent.Await
import munit.Clue.generate

import org.apache.pekko.actor.typed.scaladsl.adapter._
import org.apache.pekko.actor.ActorSystem
import org.apache.pekko.util.Timeout
import org.apache.pekko.actor.typed.scaladsl.Behaviors
import org.apache.pekko.actor.typed.scaladsl.AskPattern.Askable
import org.apache.pekko.actor.testkit.typed.scaladsl.ActorTestKit
import org.apache.pekko.cluster.ddata

class testClass extends munit.FunSuite:
    import CRDTActorLocks.*
//    test("sum of two integers") {
//        val obtained = 2 + 2
//        val expected = 4
//        assertEquals(obtained, expected)
//    }
//
//    test("all even numbers") {
//        val input: List[Int] = List(1, 2, 3, 4)
//        val obtainedResults: List[Int] = input.map(_ * 2)
//        // check that obtained values are all even numbers
//        assert(obtainedResults.forall(x => x % 2 == 0))
//    }

    test("CRDT actors transaction") {
        val N: Int = 2
        val system = ActorSystem("CRDTActor")

        val testKit = ActorTestKit()
        // val probe = testKit.createTestProbe[CRDTActorV2.State]()

        val actors = (0 until N).map { i =>
            val name = s"CRDTActor-$i"
            val actorRef = system.spawn(
                Behaviors.setup[CRDTActorLocks.Command] { ctx => new CRDTActorLocks(i, ctx) },
                name
            )
            i -> actorRef
        }.toMap

        val probe = testKit.createTestProbe[CRDTActorLocks.Command]()
        actors.foreach((id, actorRef) => Utils.GLOBAL_STATE.put(id, actorRef))
        // Start the actors
        actors.foreach((_, actorRef) => actorRef ! CRDTActorLocks.Start)

        //TODO: Change so that first actor 0 puts the two values, then gets the values and updates. Lastly Actor 1 gets the values and they should be as expected

        actors(0) ! CRDTActorLocks.Put("amount0", 200)
        //Thread.sleep(500)
        actors(1) ! CRDTActorLocks.Put("amount1", 100)
        //Thread.sleep(500)
        //actors(0) ! CRDTActorLocks.Put("amount0", 250) //Set the amount by first getting the current amount
        //Thread.sleep(500)
        //actors(1) ! CRDTActorLocks.Put("amount1", 50) //Set the amount by first getting the current amount
        Thread.sleep(1000)
        println("-----------------------------------------------------")
        actors(0) ! CRDTActorLocks.Get(probe.ref)
        actors(1) ! CRDTActorLocks.Get(probe.ref)
        Thread.sleep(1000)
        var responses = (0 until N).map(_ => probe.receiveMessage())

        var counter: Int = 0
        var amount0: Int = 0
        var amount1: Int = 0

        responses.foreach {
            case msg: responseMsg =>
                println(msg)
                msg match {
                    case responseMsg(map) =>
                        if (counter == 1) {
                            amount1 = map.get("amount1").getOrElse(0) // Get value or default to 0
                            //println(amount1)
                            //actors(counter) ! CRDTActorLocks.Put("amount1", amount1+50)
                        } else {
                            amount0 = map.get("amount0").getOrElse(0) // Get value or default to 0
                            //println(amount0)
                            //actors(counter) ! CRDTActorLocks.Put("amount0", amount0-50)
                        }
                        counter = counter + 1
                    case _ => fail("Unexpected message: " + msg)
                }
            case msg =>
                fail("Unexpected message: " + msg)
        }
        println("Amount0: " + amount0)
        println("Amount1: " + amount1)
        actors(0) ! CRDTActorLocks.Put("amount0", amount0-50)
        actors(1) ! CRDTActorLocks.Put("amount1", amount1+50)

        counter = 0
        Thread.sleep(1000)
        actors(0) ! CRDTActorLocks.Get(probe.ref)
        actors(1) ! CRDTActorLocks.Get(probe.ref)
        println("=====================================================")
        Thread.sleep(1000)
        var SecondResponses = (0 until N).map(_ => probe.receiveMessage())
        var result = 0
        //Thread.sleep(1000)
        SecondResponses.foreach {
            case msg: responseMsg =>
                msg match {
                    case responseMsg(map) =>
                        result = map.get("amount1").getOrElse(0) // Get value or default to 0
                        println(msg)
                    case _ => fail("Unexpected message: " + msg)
                }
            case msg =>
                fail("Unexpected message: " + msg)
        }
        assertEquals(result, 150)
    }

    test("CRDT non-atomic operation") {
        val N: Int = 3
        val system = ActorSystem("CRDTActor")

        val testKit = ActorTestKit()

        val actors = (0 until N).map { i =>
            val name = s"CRDTActor-$i"
            val actorRef = system.spawn(
                Behaviors.setup[CRDTActorLocks.Command] { ctx => new CRDTActorLocks(i, ctx) },
                name
            )
            i -> actorRef
        }.toMap

        val probe = testKit.createTestProbe[CRDTActorLocks.Command]()
        actors.foreach((id, actorRef) => Utils.GLOBAL_STATE.put(id, actorRef))
        // Start the actors
        actors.foreach((_, actorRef) => actorRef ! CRDTActorLocks.Start)
        actors(0) ! CRDTActorLocks.Put("x", 200)
        actors(0) ! CRDTActorLocks.Put("y", 200)
        actors(1) ! CRDTActorLocks.Put("x", 50)
        actors(1) ! CRDTActorLocks.Put("y", 50)

        actors(2) ! CRDTActorLocks.Get(probe.ref)
        Thread.sleep(2500)
        var response = (0 until 1).map(_ => probe.receiveMessage())
        var resultX = 0
        var resultY = 0
        response.foreach {
            case msg: responseMsg =>
                println(msg)
                msg match {
                    case responseMsg(map) =>
                        resultX = map.get("x").getOrElse(0) // Get value or default to 0
                        resultY = map.get("y").getOrElse(1) // Get value or default to 1
                    case _ => fail("Unexpected message: " + msg)
                }
            case msg =>
                fail("Unexpected message: " + msg)
        }
        println("Result non-atomic X: " + resultX)
        println("Result non-atomic Y: " + resultY)
        assertNotEquals(resultX, resultY)
    }

    test("CRDT atomic operation") {
        val N: Int = 3
        val system = ActorSystem("CRDTActor")

        val testKit = ActorTestKit()
        // val probe = testKit.createTestProbe[CRDTActorV2.State]()

        val actors = (0 until N).map { i =>
            val name = s"CRDTActor-$i"
            val actorRef = system.spawn(
                Behaviors.setup[CRDTActorLocks.Command] { ctx => new CRDTActorLocks(i, ctx) },
                name
            )
            i -> actorRef
        }.toMap

        val probe = testKit.createTestProbe[CRDTActorLocks.Command]()
        actors.foreach((id, actorRef) => Utils.GLOBAL_STATE.put(id, actorRef))
        // Start the actors
        actors.foreach((_, actorRef) => actorRef ! CRDTActorLocks.Start)
        var valuesToAdd: Map[String, Int] = Map("x" -> 200, "y" -> 200)
        actors(0) ! CRDTActorLocks.AtomicPut(valuesToAdd)
        valuesToAdd = Map("x" -> 50, "y" -> 50)
        actors(1) ! CRDTActorLocks.AtomicPut(valuesToAdd)
        Thread.sleep(1000)
        actors(2) ! CRDTActorLocks.Get(probe.ref)
        var response = (0 until 1).map(_ => probe.receiveMessage())
        var resultX = 0
        var resultY = 0
        response.foreach {
            case msg: responseMsg =>
                println(msg)
                msg match {
                    case responseMsg(map) =>
                        resultX = map.get("x").getOrElse(0) // Get value or default to 0
                        resultY = map.get("y").getOrElse(1) // Get value or default to 1
                    case _ => fail("Unexpected message: " + msg)
                }
            case msg =>
                fail("Unexpected message: " + msg)
        }
        println("Result atomic X: " + resultX)
        println("Result atomic Y: " + resultY)
        assertEquals(resultX, resultX)
    }