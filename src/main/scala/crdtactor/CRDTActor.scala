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

 //case object GetState extends Command //TESTING
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
 private var vi: (Int, Long) = null //TODO, should be empty
 private var ackVotes: Int = 0
 private var nackVotes: Int = 0
 private var highestK: Int = 0
 private var tempV: (Int, Long) = (0,0)
 private var ki: Int = id

 private var waitingForR: Boolean = false
 private var waitingForW: Boolean = false

 private val executionContext: ExecutionContext = ctx.system.executionContext

 private val hearBeatTimeOut = java.time.Duration.ofMillis(400)

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
              //ctx.log.info(s"CRDTActor-$id: Actor timed out")
              alive = alive.removed(actorRef)
            }
        }
      },
      executionContext)
  }

 // Note: you probably want to modify this method to be more efficient
 //TODO: atm it broadcasts the whole crdt, make it more efficient by only broadcasting what should be added!!
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

 //This is my getLease function
 private def commit(v: (Int, Long)): Unit = {
   //ctx.log.info(s"CRDTActor-$id: Inside the commit func with v: " + v)
   if (v != null) {
     if (v._2 < java.time.Instant.now().toEpochMilli && (v._2 + 50) > java.time.Instant.now().toEpochMilli) {
       ctx.log.info(s"CRDTActor-$id: Never comes here?")
       //ki = read + 1
       Thread.sleep(50)
       ctx.self ! ConsumeOperation // start consuming operations
       Behaviors.same
     }
   }
   if (v == (0,0)) { //Add check for time now later
     ctx.log.info(s"CRDTActor-$id: Not null right")
     writeFunc(ki, (id,java.time.Instant.now().toEpochMilli + 100))
   } else if (v._2 < java.time.Instant.now().toEpochMilli) {
     ctx.log.info(s"CRDTActor-$id: v._2: " + v._2)
     ctx.log.info(s"CRDTActor-$id: current time: " + java.time.Instant.now().toEpochMilli)
     writeFunc(ki, (id,java.time.Instant.now().toEpochMilli + 100))
   } 
   //else if (v._1 == id) {
   //   ctx.log.info(s"CRDTActor-$id: renew")
   //   writeFunc(ki, (id,java.time.Instant.now().toEpochMilli() + 100))
   // } 
   else {
     ctx.self ! ConsumeOperation // continue consuming operations, loops sortof
     Behaviors.same
   }
 }

 private def writeCommit(): Unit = {
   //SYNC then do this
   //ctx.log.info(s"CRDTActor-$id: acks: $ackVotes - nacks: $nackVotes")
   ctx.log.info(s"CRDTActor-$id: Got to commit and ki: " + ki)
   val key = Utils.randomString()
   val value = Utils.randomInt()
   ctx.log.info(s"CRDTActor-$id: Consuming operation $key -> $value")
   crdtstate = crdtstate.put(selfNode, key, value)
   ctx.log.info(s"CRDTActor-$id: CRDT state: $crdtstate")
   broadcastAndResetDeltas()
   ctx.self ! ConsumeOperation // continue consuming operations, loops sortof
   Behaviors.same
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
     // ctx.self ! ConsumeOperation // start consuming operations
     Behaviors.same

   case ConsumeOperation =>
     //ctx.log.info(s"CRDTActor-$id trying to get lease with ki: " + ki)
     readFunc(ki)
     Behaviors.same

   case HeartBeat(from) =>
     ctx.log.info(s"CRDTActor-$id: got a heartbeat from: " + from)
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
     //ctx.log.info(s"CRDTActor-$id: Received delta from ${from.path.name}")
     // Merge the delta into the local CRDT state
     crdtstate = crdtstate.mergeDelta(delta.asInstanceOf) // do you trust me? Look below
     //AsInstanceOf[ReplicatedDelta[ReplicatedData] 
     //if you don't provide the type the worst case is that they will cast to nothing, so it will break.
     //Do safe casting, if you can't cast it, then you have a problem and discard.
     ctx.log.info(s"CRDTActor-$id: Merged CRDT state: $crdtstate")
     Behaviors.same
   
   case Read(k, sender) =>
     //ctx.log.info(s"CRDTActor-$id: got a read request with: " + k + " but the highest write: " + write + " and read: " + read)
     // println("This is our k value in Read event: " + k)
     // println("And our write: " + write + ", and read: " + read)
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
         // nackVotes = 0
         // ackVotes = 0
         //abort
         //Alt ki + 1
         //ctx.log.info(s"CRDTActor-$id:got denied with ki of: " + ki)
         waitingForR = false
         ctx.self ! ConsumeOperation // start consuming operations
         //Behaviors.same
       }
     }
     Behaviors.same
   

   case AckRead(k, writej, v) => 
     if (waitingForR) {
       //ctx.log.info(s"CRDTActor-$id:got an ackread with ki of: " + ki)
       ackVotes = ackVotes + 1
       if (writej > highestK) {
         highestK = writej
         tempV = v
       }
       if (ackVotes >= ((alive.size+1)/2) && nackVotes == 0) {
         // ackVotes = 0
         // nackVotes = 0
         waitingForR = false
         commit(tempV)
         // ctx.self ! ConsumeOperation // continue consuming operations, loops sortof
         // Behaviors.same
       } else if(ackVotes >= ((alive.size+1)/2) && nackVotes > 0) {
         waitingForR = false
         ctx.self ! ConsumeOperation // continue consuming operations, loops sortof
         Behaviors.same
       }
     }
     Behaviors.same
   

   case NackWrite(k) => 
     if (waitingForW) {
       nackVotes = nackVotes + 1
       if (nackVotes >= ((alive.size+1)/2)) {
         // nackVotes = 0
         // ackVotes = 0
         //abort
         //ki = write + 1
         waitingForR = false
         ctx.self ! ConsumeOperation // start consuming operations
         //Behaviors.same
       }
     }
     Behaviors.same

   case AckWrite(k) => 
     if (waitingForW) {
       ackVotes = ackVotes + 1
       if (ackVotes >= ((alive.size+1)/2) && nackVotes == 0) {
         // ackVotes = 0
         // nackVotes = 0
         waitingForR = false
         writeCommit()
       } else if(ackVotes >= ((alive.size+1)/2) && nackVotes > 0) {
         //Restart
         waitingForR = false
         ctx.self ! ConsumeOperation // continue consuming operations, loops sortof
         Behaviors.same
       }
     }
     Behaviors.same

 Behaviors.same
}
