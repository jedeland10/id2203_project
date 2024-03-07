package crdtactor

import org.apache.pekko.actor.typed.scaladsl.ActorContext
import org.apache.pekko.actor.typed.scaladsl.AbstractBehavior
import org.apache.pekko.actor.typed.scaladsl.Behaviors
import org.apache.pekko.actor.typed.Behavior
import org.apache.pekko.cluster.ddata
import org.apache.pekko.cluster.ddata.ReplicatedDelta
import org.apache.pekko.actor.typed.ActorRef

import scala.collection.mutable
import scala.collection.mutable.Queue
import scala.concurrent.ExecutionContext

object CRDTActorLocks {

  sealed trait Operation
  case class opPut(key: String, value: Int) extends Operation
  case class opInc(key: String) extends Operation
  // The type of messages that the actor can handle
  sealed trait Command

  // Messages containing the CRDT delta state exchanged between actors
  case class DeltaMsg(from: ActorRef[Command], delta: ReplicatedDelta)
    extends Command

  // Triggers the actor to start the computation (do this only once!)
  case object Start extends Command

  // Triggers the actor to consume an operation (do this repeatedly!)
  case object ConsumeOperation extends Command

  case class Put(key: String, value: Int) extends Command //Add a value to the CRDT

  //case class Get(replyTo: ActorRef[ddata.LWWMap[String, Int]]) extends Command //Request the state of the CRDT

  case class Get(replyTo: ActorRef[Command]) extends Command

  case class AcquireLock(requester: ActorRef[Command]) extends Command //Request a lock from another actor

  case class LockAcquired(sender: ActorRef[Command]) extends CRDTActorLocks.Command //Receive a lock from another actor

  case object LockReleased extends Command

  case class responseMsg(msg: ddata.LWWMap[String, Int]) extends Command

  case class AtomicPut(mapInput: Map[String, Int]) extends Command

  case class AtomicIncrement(key: String) extends Command

  case class Increment(key: String) extends Command

  case class HeartBeat(from: ActorRef[Command]) extends Command

  case class Sleep(time: Long) extends Command

  //case class LockReleased() //Release a lock to another actor

}

import CRDTActorLocks.*

class CRDTActorLocks(
                      id: Int,
                      ctx: ActorContext[Command]
                    ) extends AbstractBehavior[Command](ctx) {
  // The CRDT state of this actor, mutable var as LWWMap is immutable
  private var crdtstate = ddata.LWWMap.empty[String, Int]

  // The CRDT address of this actor/node, used for the CRDT state to identify the nodes
  private val selfNode = Utils.nodeFactory()

  private var hasLock = true //If the actor has its own lock
  private var lockQueue = mutable.Queue.empty[ActorRef[Command]] //Queue of actors waiting for the lock
  private var mapOfLocks = Map.empty[ActorRef[Command], Boolean] //Map of actors and their locks
  var keyToBe = "" //Key to be added to the CRDT
  var valueToBe = 0 //Value to be added to the CRDT
  private var atomicMap = Map.empty[String, Int] //Map of key-value pairs to be added to the CRDT

  private val opQueue = mutable.Queue.empty[Operation]

  private var currentHolder = ctx.self

  // Hack to get the actor references of the other actors, check out `lazy val`
  // Careful: make sure you know what you are doing if you are editing this code
  private lazy val others =
  Utils.GLOBAL_STATE.getAll[Int, ActorRef[Command]]()

  private val executionContext: ExecutionContext = ctx.system.executionContext

  private val hearBeatTimeOut = java.time.Duration.ofMillis(400)

  private var alive = Map.empty[ActorRef[Command], Long]

  private def startHeartbeatScheduler(): Unit = {
    ctx.system.scheduler.scheduleWithFixedDelay(
      java.time.Duration.ofMillis(200),
      java.time.Duration.ofMillis(200),
      () => alive.foreach((actorRef, _) => actorRef ! HeartBeat(ctx.self)),
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
  private def broadcastAndResetDeltas(): Unit =
    val deltaOption = crdtstate.delta
    deltaOption match
      case None => ()
      case Some(delta) =>
        crdtstate = crdtstate.resetDelta // May be omitted
        alive.foreach { //
          (actorRef, _) =>
            actorRef !
              DeltaMsg(ctx.self, delta)
        }

  private def sendDelta(actorRef: ActorRef[Command]): Unit =
    val deltaOption = crdtstate.delta
    deltaOption match
      case None => ()
      case Some(delta) =>
        crdtstate = crdtstate.resetDelta // May be omitted
        actorRef ! DeltaMsg(ctx.self, delta)


  // This is the event handler of the actor, implement its logic here
  // Note: the current implementation is rather inefficient, you can probably
  // do better by not sending as many delta update messages
  override def onMessage(msg: Command): Behavior[Command] = msg match
    case Start =>
      ctx.log.info(s"CRDTActor-$id started")
      others.filter((_, ref) => ref != ctx.self).foreach((_, actorRef) =>
        alive = alive.updated(actorRef, java.time.Instant.now().toEpochMilli + hearBeatTimeOut.toMillis))
      startHeartbeatScheduler()
      startTimeoutScheduler()
      Behaviors.same

    case ConsumeOperation =>
      ctx.self ! ConsumeOperation // continue consuming operations, loops sortof
      Behaviors.same

    case DeltaMsg(from, delta) =>
      crdtstate = crdtstate.mergeDelta(delta.asInstanceOf) // do you trust me?
      Behaviors.same

    case HeartBeat(from) =>
      if (!alive.contains(from)) {
        sendDelta(from)
      }
      alive = alive.updated(from, java.time.Instant.now().toEpochMilli + hearBeatTimeOut.toMillis)
      Behaviors.same

    case Sleep(time) =>
      Thread.sleep(time)
      Behaviors.same

    case Put(key, value) => //Tries to add a value into the CRDT
      keyToBe = key
      valueToBe = value
      opQueue.enqueue(opPut(key, value))
      // Request locks from all other actors
      alive.foreach { (actorRef, _) =>
        actorRef ! AcquireLock(ctx.self)
      }
      Behaviors.same

    case Get(replyTo) =>
      replyTo ! responseMsg(crdtstate) //Send the state to the requester
      ctx.log.info(s"$crdtstate")
      Behaviors.same

    case AcquireLock(requester) =>
      if (requester != ctx.self) {
        if (hasLock) {
          ctx.log.info(s"CRDTActor-$id: gave $requester lock")
          requester ! LockAcquired(ctx.self) //Give the lock to the requester
          currentHolder = requester
          hasLock = false
        } else if (!lockQueue.contains(requester) && currentHolder != requester) {
          ctx.log.info(s"CRDTActor-$id: added $requester to lock queue")
          lockQueue.enqueue(requester) //Add the requester to the queue
        }
      }
      Behaviors.same

    case LockAcquired(sender) =>
      ctx.log.info(s"CRDTActor-$id: got lock from $sender")
      mapOfLocks = mapOfLocks + (sender -> true)
      if (mapOfLocks.size == alive.size) {
        ctx.log.info(s"CRDTActor-$id: I have all the locks!")
        ctx.log.info(s"CRDTActor-$id: writing: $opQueue")
        if (atomicMap.nonEmpty) {
          atomicMap.foreach { (key, value) =>
            crdtstate = crdtstate.put(selfNode, key, value)
          }
          atomicMap = Map.empty[String, Int]
        }

        while (opQueue.nonEmpty) {
          val op = opQueue.dequeue
          op match {
            case opPut(key, value) =>
              crdtstate = crdtstate.put(selfNode, key, value)
              ctx.log.info(s"CRDTActor-$id current state $crdtstate")
            case opInc(key) =>
              crdtstate = crdtstate.put(selfNode, key, crdtstate.get(key).getOrElse(0) + 1)
              ctx.log.info(s"CRDTActor-$id current state $crdtstate")
          }
        }
        broadcastAndResetDeltas()
        mapOfLocks.foreach { (actorRef, _) =>
          actorRef ! LockReleased
        }
        mapOfLocks = Map.empty[ActorRef[Command], Boolean]
      } else if (hasLock) {
          alive.filter((actorRef, _) => !mapOfLocks.contains(actorRef)).foreach { (actorRef, _) =>
            actorRef ! AcquireLock(ctx.self)
          }
      }
      Behaviors.same

    case LockReleased =>
      hasLock = true
      currentHolder = ctx.self
      ctx.log.info(s"CRDTActor-$id: got my lock back")
      if (lockQueue.nonEmpty) {
        val actorRef = lockQueue.dequeue
        actorRef ! LockAcquired(actorRef)
        currentHolder = actorRef
      }
      Behaviors.same

    //Should take a an array of keys and values
    case AtomicPut(mapInput) =>
      atomicMap = mapInput
      alive.foreach { (actorRef, _) =>
        actorRef ! AcquireLock(ctx.self)
      }
      Behaviors.same

    case Increment(key) =>
      opQueue.enqueue(opInc(key))
      alive.foreach { (actorRef, _) =>
        actorRef ! AcquireLock(ctx.self)
      }
      Behaviors.same

  Behaviors.same
}