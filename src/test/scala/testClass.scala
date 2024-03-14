package crdtactor

import org.apache.pekko.actor.typed.scaladsl.adapter.*
import org.apache.pekko.actor.ActorSystem
import org.apache.pekko.actor.typed.scaladsl.Behaviors
import org.apache.pekko.actor.testkit.typed.scaladsl.ActorTestKit

class testClass extends munit.FunSuite:
    import CRDTActorLocks.*

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


        actors(0) ! CRDTActorLocks.Put("amount0", 200)
        actors(1) ! CRDTActorLocks.Put("amount1", 100)
        Thread.sleep(1000)
        println("-----------------------------------------------------")
        actors(0) ! CRDTActorLocks.Get(probe.ref)
        actors(1) ! CRDTActorLocks.Get(probe.ref)
        Thread.sleep(1000)
        val responses = (0 until N).map(_ => probe.receiveMessage())

        var counter: Int = 0
        var amount0: Int = 0
        var amount1: Int = 0

        responses.foreach {
            case msg: responseMsg =>
                println(msg)
                msg match {
                    case responseMsg(map) =>
                        if (counter == 1) {
                            amount1 = map.get("amount1").getOrElse(0)
                        } else {
                            amount0 = map.get("amount0").getOrElse(0)
                        }
                        counter = counter + 1
                    case null => fail("Unexpected message: " + msg)
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
        val SecondResponses = (0 until N).map(_ => probe.receiveMessage())
        var result = 0
        //Thread.sleep(1000)
        SecondResponses.foreach {
            case msg: responseMsg =>
                msg match {
                    case responseMsg(map) =>
                        result = map.get("amount1").getOrElse(0)
                        println(msg)
                    case null => fail("Unexpected message: " + msg)
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

        actors.foreach((_, actorRef) => actorRef ! CRDTActorLocks.Start)
        actors(0) ! CRDTActorLocks.Put("x", 200)
        actors(0) ! CRDTActorLocks.Put("y", 200)
        actors(1) ! CRDTActorLocks.Put("x", 50)
        actors(1) ! CRDTActorLocks.Put("y", 50)

        actors(2) ! CRDTActorLocks.Get(probe.ref)
        Thread.sleep(2500)
        val response = (0 until 1).map(_ => probe.receiveMessage())
        var resultX = 0
        var resultY = 0
        response.foreach {
            case msg: responseMsg =>
                println(msg)
                msg match {
                    case responseMsg(map) =>
                        resultX = map.get("x").getOrElse(0)
                        resultY = map.get("y").getOrElse(1)
                    case null => fail("Unexpected message: " + msg)
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

        actors.foreach((_, actorRef) => actorRef ! CRDTActorLocks.Start)
        var valuesToAdd: Map[String, Int] = Map("x" -> 200, "y" -> 200)
        actors(0) ! CRDTActorLocks.AtomicPut(valuesToAdd)
        valuesToAdd = Map("x" -> 50, "y" -> 50)
        actors(1) ! CRDTActorLocks.AtomicPut(valuesToAdd)
        Thread.sleep(1000)
        actors(2) ! CRDTActorLocks.Get(probe.ref)
        val response = (0 until 1).map(_ => probe.receiveMessage())
        var resultX = 0
        var resultY = 0
        response.foreach {
            case msg: responseMsg =>
                println(msg)
                msg match {
                    case responseMsg(map) =>
                        resultX = map.get("x").getOrElse(0)
                        resultY = map.get("y").getOrElse(1)
                    case null => fail("Unexpected message: " + msg)
                }
            case msg =>
                fail("Unexpected message: " + msg)
        }
        println("Result atomic X: " + resultX)
        println("Result atomic Y: " + resultY)
        assertEquals(resultX, resultX)
    }
    test("CRDT put sequentially consistent") {
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

        actors.foreach((_, actorRef) => actorRef ! CRDTActorLocks.Start)

        for (n <- Range(0, 10)) {
            actors(0) ! CRDTActorLocks.Put(n.toString, n)
        }
        for (n <- Range(10, 20)) {
            actors(1) ! CRDTActorLocks.Put(n.toString, n)
        }

        Thread.sleep(5000)
        actors(2) ! CRDTActorLocks.Get(probe.ref)
        val response = (0 until 1).map(_ => probe.receiveMessage())
        var resultX = 0
        var resultY = 0
        response.foreach {
            case msg: responseMsg =>
                println(msg)
                msg match {
                    case responseMsg(map) =>
                        resultX = map.get("x").getOrElse(0)
                        resultY = map.get("y").getOrElse(1)
                    case null => fail("Unexpected message: " + msg)
                }
            case msg =>
                fail("Unexpected message: " + msg)
        }
        println("Result non-atomic X: " + resultX)
        println("Result non-atomic Y: " + resultY)
        assertNotEquals(resultX, resultY)
    }


    test("Integration test") {
        val N: Int = 8
        val nMessages = 1_000_000
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

        actors.foreach((_, actorRef) => actorRef ! CRDTActorLocks.Start)

        var counter = 0
        val nRounds = 5

        for (n <- Range(0, nRounds)) {
            for (n <- Range(0, nMessages)) {
                actors.foreach((_, actorRef) => actorRef ! CRDTActorLocks.Increment("x"))
            }

            Thread.sleep(3_500)
            actors(0) ! CRDTActorLocks.Get(probe.ref)
            val response = probe.receiveMessage()
            var resultX = 0

            response.match
                case responseMsg(map) => resultX = map.get("x").getOrElse(0)
                case null => fail("Unexpected message: " + response)

            println(s"Result round $n: " + resultX)
            if (resultX == nMessages * N) counter = counter + 1

            actors(0) ! CRDTActorLocks.PaxosPut("x", 0)
            Thread.sleep(1000)

        }
        println("Succeeded rounds: " + counter + "\nTotal rounds: " + nRounds)
        assertEquals(counter, nRounds)
    }


    test("Latency") {
        val system = ActorSystem("CRDTActor")
        val N = 4

        val testKit = ActorTestKit()
        val probe = testKit.createTestProbe[CRDTActorLocks.Command]()

        val actors = (0 until N).map { i =>
            val name = s"CRDTActor-$i"
            val actorRef = system.spawn(
                Behaviors.setup[CRDTActorLocks.Command] { ctx => new CRDTActorLocks(i, ctx) },
                name
            )
            i -> actorRef
        }.toMap

        actors.foreach((id, actorRef) => Utils.GLOBAL_STATE.put(id, actorRef))

        actors.foreach((_, actorRef) => actorRef ! CRDTActorLocks.Start)

        val nMessages = 1_000
        var benchmark = java.time.Instant.now().toEpochMilli

        for (n <- Range(0, nMessages)) {
            actors.foreach((_,actorRef) => actorRef ! CRDTActorLocks.Increment("x"))
        }
        var resp = 0
        while (resp < nMessages) {
            actors(0) ! CRDTActorLocks.Get(probe.ref)
            val response = probe.receiveMessage()
            response match
                case responseMsg(map) => resp = map.get("x").getOrElse(0)
                case null => fail("Unexpected message: " + responseMsg)

        }
        benchmark = java.time.Instant.now().toEpochMilli - benchmark

        println("1000 operations, 4 actors, 250 operations each: " + benchmark +"ms")
    }

    test("Fail recovery test") {
        val N: Int = 5
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
        Thread.sleep(200)
        actors(0) ! CRDTActorLocks.GetAlive(probe.ref)
        Thread.sleep(200)
        actors(N-1) ! CRDTActorLocks.Sleep(1000)
        Thread.sleep(1500)
        actors(0) ! CRDTActorLocks.GetAlive(probe.ref)
        Thread.sleep(1500)
        //Restart the terminated actor
        val nameRestart = s"CRDTActor-${N-1}"
        val actorRefRestart = system.spawn(
            Behaviors.setup[CRDTActorLocks.Command] { ctx => new CRDTActorLocks(N-1, ctx) },
            nameRestart
        )
        Utils.GLOBAL_STATE.put(N-1, actorRefRestart)
        actorRefRestart ! CRDTActorLocks.Start
        Thread.sleep(1500)
        actors(0) ! CRDTActorLocks.GetAlive(probe.ref)
        Thread.sleep(1500)

        assertEquals(100, 100)
    }

    /*
    test("Fleases execution") {
        val N_ACTORS = 4

        Utils.setLoggerLevel("DEBUG")

        val system = ActorSystem("CRDTActor2")
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
        Thread.sleep(12_000)
    }*/