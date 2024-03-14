package crdtactor

import org.apache.pekko.actor.typed.scaladsl.ActorContext
import org.apache.pekko.actor.typed.scaladsl.AbstractBehavior
import org.apache.pekko.actor.typed.scaladsl.Behaviors
import org.apache.pekko.actor.typed.Behavior
import org.apache.pekko.cluster.ddata
import org.apache.pekko.cluster.ddata.ReplicatedDelta
import org.apache.pekko.actor.typed.ActorRef
import org.apache.pekko.actor.Cancellable

import scala.concurrent.ExecutionContext


object CRDTActor {
 // The type of messages that the actor can handle
 sealed trait Command

 // Messages containing the CRDT delta state exchanged between actors
 case class DeltaMsg(from: ActorRef[Command], delta: ReplicatedDelta)
     extends Command

 // Triggers the actor to start the computation (do this only once!)
 case object Start extends Command

 // Triggers the actor to consume an operation (do this repeatedly!)
 case object ConsumeOperation extends Command

 case class Read(k: Int, sender: ActorRef[Command]) extends Command

 case class Write(k: Int, v: (Int, Long) , sender: ActorRef[Command]) extends Command

 case class AckRead(k: Int, writej: Int, v: (Int, Long)) extends Command

 case class NackRead(k: Int) extends Command

 case class AckWrite(k: Int) extends Command

 case class NackWrite(k: Int) extends Command

 case class HeartBeat(from: ActorRef[Command]) extends Command

 case class Sleep(time: Long) extends Command
  
}

import CRDTActor.*

class CRDTActor(
   id: Int,
   ctx: ActorContext[Command]
) extends AbstractBehavior[Command](ctx) {
 // The CRDT state of this actor, mutable var as LWWMap is immutable
 private var crdtstate = ddata.LWWMap.empty[String, Int]

 // The CRDT address of this actor/node, used for the CRDT state to identify the nodes
 private val selfNode = Utils.nodeFactory()

 // Hack to get the actor references of the other actors, check out `lazy val`
 // Careful: make sure you know what you are doing if you are editing this code
 private lazy val others =
   Utils.GLOBAL_STATE.getAll[Int, ActorRef[Command]]()

 private var write: Int = 0
 private var read: Int = 0
 private var vi: (Int, Long) = null
 private var ackVotes: Int = 0
 private var nackVotes: Int = 0
 private var highestK: Int = 0
 private var tempV: (Int, Long) = (0,0)
 private var ki: Int = id

 private var waitingForR: Boolean = false
 private var waitingForW: Boolean = false

 private val executionContext: ExecutionContext = ctx.system.executionContext

 private val hearBeatTimeOut = java.time.Duration.ofMillis(400)

 private var startTime: Long = java.time.Instant.now().toEpochMilli

 private var alive = Map.empty[ActorRef[Command], Long]
  private def startHeartbeatScheduler(): Unit = {
    ctx.system.scheduler.scheduleWithFixedDelay(
      java.time.Duration.ofMillis(200),
      java.time.Duration.ofMillis(200),
      () => alive.foreach((actorRef,_) => actorRef ! HeartBeat(ctx.self)),
      executionContext)
  }
  private def startTimeoutScheduler(): Unit = {
    ctx.system.scheduler.scheduleWithFixedDelay(
      java.time.Duration.ofMillis(500),
      java.time.Duration.ofMillis(500),
      () => {
        alive.foreach {
          case (actorRef, expectedResponseTime) =>
            if (expectedResponseTime < java.time.Instant.now().toEpochMilli) {
              alive = alive.removed(actorRef)
            }
        }
      },
      executionContext)
  }
  
 private def broadcastAndResetDeltas(): Unit =
   val deltaOption = crdtstate.delta
   deltaOption match
     case None => ()
     case Some(delta) =>
       crdtstate = crdtstate.resetDelta // May be omitted
       alive.foreach { //
         (actorRef, _) =>
           if (actorRef != ctx.self) {
             actorRef !
               DeltaMsg(ctx.self, delta)
           }
       }

 private def readFunc(k: Int): Unit = {
   ackVotes = 0
   nackVotes = 0
   highestK = 0
   waitingForR = true
   alive.foreach {
         (actorRef, _) =>
           if (actorRef != ctx.self) {
             actorRef ! Read(k, ctx.self)
           }
       }
 }

 private def writeFunc(k: Int, v: (Int,Long)): Unit = {
   ackVotes = 0
   nackVotes = 0
   waitingForW = true
   alive.foreach {
         (actorRef, _) =>
           if (actorRef != ctx.self) {
             actorRef ! Write(k, v, ctx.self)
           }
       }
 }
  
 private def commit(v: (Int, Long)): Unit = {
   if (v != null) {
     if (v._2 < java.time.Instant.now().toEpochMilli && (v._2 + 50) > java.time.Instant.now().toEpochMilli) {
       Thread.sleep(50)
       ctx.self ! ConsumeOperation // start consuming operations
       Behaviors.same
     }
   }
   if (v == (0,0)) { //Add check for time now later
     writeFunc(ki, (id,java.time.Instant.now().toEpochMilli + 100))
   } else if (v._2 < java.time.Instant.now().toEpochMilli) {
     writeFunc(ki, (id,java.time.Instant.now().toEpochMilli + 100))
   }
   else {
     ctx.self ! ConsumeOperation
     Behaviors.same
   }
 }

 private def writeCommit(): Unit = {
   val key = "x"
   val value = Utils.randomInt()
   val currentVal: Int = crdtstate.get("x").getOrElse(0)
   if (currentVal == 100) {
     val endTime: Long = java.time.Instant.now().toEpochMilli
     val timeElapsed = endTime - startTime
     ctx.log.info(s"CRDTActor-$id: Finished adding 100 values in: " + timeElapsed + " ms")
     ctx.log.info(s"--------------------------------------------------------------------------------------------")
     Behaviors.stopped
   } else if (currentVal > 100) {
     Behaviors.stopped
   } else {
     crdtstate = crdtstate.put(selfNode, key, currentVal+1)
     ctx.log.info(s"CRDTActor-$id: CRDT state: $crdtstate")
     broadcastAndResetDeltas()
     ctx.self ! ConsumeOperation // continue consuming operations, loops sortof
     Behaviors.same
   }
 }

 // This is the event handler of the actor, implement its logic here
 // Note: the current implementation is rather inefficient, you can probably
 // do better by not sending as many delta update messages
 override def onMessage(msg: Command): Behavior[Command] = msg match
   case Start =>
     ctx.log.info(s"CRDTActor-$id started")
     others.filter((_,ref) => ref != ctx.self).foreach((_, actorRef) =>
       alive = alive.updated(actorRef, java.time.Instant.now().toEpochMilli + hearBeatTimeOut.toMillis))
     startHeartbeatScheduler()
     startTimeoutScheduler()
     ctx.self ! ConsumeOperation // start consuming operations
     Behaviors.same

   case ConsumeOperation =>
     if (ki == id) {
       startTime = java.time.Instant.now().toEpochMilli
     }
     readFunc(ki)
     Behaviors.same

   case HeartBeat(from) =>
     if (!alive.contains(from)) {
       ctx.log.info(s"CRDTActor-$id: rejoined, alive: " + alive)
       ctx.log.info(s"CRDTActor-$id: rejoined, ref: " + from)
     }
     alive = alive.updated(from, java.time.Instant.now().toEpochMilli + hearBeatTimeOut.toMillis)
     Behaviors.same

   case Sleep(time) =>
      Thread.sleep(time)
      Behaviors.same
   

   case DeltaMsg(from, delta) =>
     crdtstate = crdtstate.mergeDelta(delta.asInstanceOf) // do you trust me? Look below
     Behaviors.same
   
   case Read(k, sender) =>
     if(write >= k || read >= k) {
       sender ! NackRead(k)
     } else {
       read = k
       sender ! AckRead(k,write,vi)
     }
     Behaviors.same
   

   case Write(k, v, sender) => 
     if (write > k || read > k) {
       sender ! NackWrite(k)
     } else {
       write = k
       vi = v
       sender ! AckWrite(k)
     }
     Behaviors.same
   

   case NackRead(k) => 
     if (waitingForR) {
       nackVotes = nackVotes + 1
       if (nackVotes >= ((alive.size+1)/2)) {
         ki = read + 1
         waitingForR = false
         ctx.self ! ConsumeOperation // start consuming operations
         //Behaviors.same
       }
     }
     Behaviors.same
   

   case AckRead(k, writej, v) => 
     if (waitingForR) {
       ackVotes = ackVotes + 1
       if (writej > highestK) {
         highestK = writej
         tempV = v
       }
       if (ackVotes >= ((alive.size+1)/2) && nackVotes == 0) {
         waitingForR = false
         commit(tempV)
         
       } else if(ackVotes >= ((alive.size+1)/2) && nackVotes > 0) {
         waitingForR = false
         ctx.self ! ConsumeOperation
         Behaviors.same
       }
     }
     Behaviors.same
   

   case NackWrite(k) => 
     if (waitingForW) {
       nackVotes = nackVotes + 1
       if (nackVotes >= ((alive.size+1)/2)) {
         waitingForR = false
         ctx.self ! ConsumeOperation // start consuming operations
       }
     }
     Behaviors.same

   case AckWrite(k) => 
     if (waitingForW) {
       ackVotes = ackVotes + 1
       if (ackVotes >= ((alive.size+1)/2) && nackVotes == 0) {
         waitingForR = false
         writeCommit()
       } else if(ackVotes >= ((alive.size+1)/2) && nackVotes > 0) {
         waitingForR = false
         ctx.self ! ConsumeOperation // continue consuming operations, loops sortof
         Behaviors.same
       }
     }
     Behaviors.same

 Behaviors.same
}
