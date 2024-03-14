package crdtactor

import org.apache.pekko.actor.typed.scaladsl.ActorContext
import org.apache.pekko.actor.typed.scaladsl.AbstractBehavior
import org.apache.pekko.actor.typed.scaladsl.Behaviors
import org.apache.pekko.actor.typed.Behavior
import org.apache.pekko.cluster.ddata
import org.apache.pekko.cluster.ddata.ReplicatedDelta
import org.apache.pekko.actor.typed.ActorRef

import scala.collection.mutable
import scala.collection.mutable.{ListBuffer, Queue}
import scala.concurrent.ExecutionContext
import scala.util.Random

object CRDTActorLocks {

  sealed trait Operation
  case class opPut(key: String, value: Int) extends Operation
  case class opInc(key: String) extends Operation
  // The type of messages that the actor can handle
  sealed trait Command

  // Messages containing the CRDT delta state exchanged between actors
  case class DeltaMsg(from: ActorRef[Command], delta: ReplicatedDelta) extends Command

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
  case class NoLockInc(key: String) extends Command

  case class PaxosPut(key: String, value: Int) extends Command //Add a value to the CRDT

  case class HeartBeat(from: ActorRef[Command]) extends Command

  case class Sleep(time: Long) extends Command

  case object ReleaseLock extends Command

  // Consensus messages
  case class C_Decide(value: Any) extends Command
  case class C_Propose(value: Any) extends Command;
  case class Prepare(src: ActorRef[Command], proposalBallot: (Int, Int)) extends Command

  case class Promise(src: ActorRef[Command], promiseBallot: (Int, Int), acceptedBallot: (Int, Int), acceptedValue: Option[Any]) extends Command

  case class Accept(src: ActorRef[Command], accBallot: (Int, Int), proposedValue: Any) extends Command

  case class Accepted(src: ActorRef[Command], acceptedBallot: (Int, Int)) extends Command

  case class Nack(src: ActorRef[Command], ballot: (Int, Int)) extends Command

  case class Decided(src: ActorRef[Command], decidedValue: Any) extends Command
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
  private var keyToBe = "" //Key to be added to the CRDT
  private var valueToBe = 0 //Value to be added to the CRDT
  private var atomicMap = Map.empty[String, Int] //Map of key-value pairs to be added to the CRDT

  private val opQueue = mutable.Queue.empty[Operation]
  private var currentHolder = ctx.self

  // Hack to get the actor references of the other actors, check out `lazy val`
  // Careful: make sure you know what you are doing if you are editing this code
  private lazy val others =
  Utils.GLOBAL_STATE.getAll[Int, ActorRef[Command]]()

  private val hearBeatTimeOut = java.time.Duration.ofMillis(400)
  private var alive = Map.empty[ActorRef[Command], Long]

  // Consensus variables
  //Proposer State
  private var round: Int = 0
  private var proposedValue: Option[Any] = None
  private var promises: ListBuffer[((Int, Int), Option[Any])] = ListBuffer.empty
  private var numOfAccepts: Int = 0
  private var decided: Boolean = false
  //Acceptor State
  private var promisedBallot: (Int, Int) = (0, 0)
  private var acceptedBallot: (Int, Int) = (0, 0)
  private var acceptedValue: Option[Any] = None

  // Propose timer
  private var proposeScheduled: Boolean = false
  private var proposing: Boolean = false

  private def startHeartbeatScheduler(): Unit = {
    ctx.system.scheduler.scheduleWithFixedDelay(
      java.time.Duration.ofMillis(200),
      java.time.Duration.ofMillis(200),
      () => alive.foreach((actorRef, _) => actorRef ! HeartBeat(ctx.self)),
      ctx.system.executionContext)
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
      ctx.system.executionContext)
  }

  private def schedulePropose(): Unit = {
    if(!proposeScheduled && opQueue.nonEmpty) {
      proposeScheduled = true
      ctx.system.scheduler.scheduleOnce(
        java.time.Duration.ofMillis(50),
        () => {
          propose()
          proposeScheduled = false
        },
        ctx.system.executionContext)
    }
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

  private def propose(): Unit = {
    if(!decided && opQueue.nonEmpty) {
      proposedValue = Some(ctx.self)
      round += 1
      promises = ListBuffer.empty
      numOfAccepts = 0
      ctx.self ! Prepare(ctx.self, (round, id))
      alive.foreach((actorRef, _) => actorRef ! Prepare(ctx.self, (round, id)))
    }
  }

  private def resetForNextRound(): Unit = {
    //promises = ListBuffer.empty
    //numOfAccepts = 0
    decided = false
    acceptedValue = None
  }

  private def writeCrdt(): Unit = {
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
        case opInc(key) =>
          crdtstate = crdtstate.put(selfNode, key, crdtstate.get(key).getOrElse(0) + 1)
      }
    }
  }


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
      schedulePropose()
      Behaviors.same

    case PaxosPut(key, value) =>
      opQueue.enqueue(opPut(key, value))
      schedulePropose()
      Behaviors.same


    case NoLockInc(key) =>
      crdtstate = crdtstate.put(selfNode, key, crdtstate.get(key).getOrElse(0) + 1)
      ctx.log.info(s"CRDTActor-$id: incremented $key")
      ctx.log.info(s"CRDTActor-$id: current state $crdtstate")
      broadcastAndResetDeltas()
      Behaviors.same


    case Prepare(src, proposalBallot) =>
      if (promisedBallot._1 < proposalBallot._1) {
        promisedBallot = proposalBallot
        src ! Promise(ctx.self, promisedBallot, acceptedBallot, acceptedValue)
      }
      else {
        src ! Nack(ctx.self, proposalBallot)
      }
      Behaviors.same

    case Accept(src, accBallot, proposedValue) =>
      if (promisedBallot._1 <= accBallot._1) {
        promisedBallot = accBallot
        acceptedBallot = accBallot
        acceptedValue = Some(proposedValue)
        src ! Accepted(ctx.self, acceptedBallot)
      }
      else {
        src ! Nack(ctx.self, accBallot)
      }
      Behaviors.same

    case Decided(src, dec) =>
      if (!decided) {
        ctx.self ! C_Decide(dec)
        alive.foreach((actorRef, _) => actorRef ! C_Decide(dec))
        decided = true
      }
      Behaviors.same

    case Promise(src, promiseBallot, acceptedBallot, acceptedValue) =>
      //ctx.log.info(s"CRDTActor-$id: received promise from $src, promiseBallot: $promiseBallot, acceptedBallot: $acceptedBallot, acceptedValue: $acceptedValue, round: $round, id: $id")
      if ((round, id) == promiseBallot) {
        val acceptedElement: ((Int, Int), Option[Any]) = (acceptedBallot, acceptedValue)

        promises += acceptedElement
        //ctx.log.info(s"CRDTActor-$id: $round,$id = $promiseBallot, promises.size: ${promises.size}, alive/2 + 1: ${alive.size / 2 + 1}")
        if (promises.size == (alive.size + 1)/ 2 + 1) {
          val (maxBallot, value) = promises.maxBy(_._1._2)
          //ctx.log.info(s"CRDTActor-$id: promises $promises")
          value match
            case None => proposedValue = proposedValue
            case Some(v) => proposedValue = Some(v)

          //ctx.log.info(s"CRDTActor-$id: send accept on $proposedValue")
          ctx.self ! Accept(ctx.self, (round, id), proposedValue.get)
          alive.foreach((actorRef, _) => actorRef ! Accept(ctx.self, (round, id), proposedValue.get))
        }
      }
      Behaviors.same

    case Accepted(src, acceptedBallot) =>
      if ((round, id) == acceptedBallot) {
        numOfAccepts += 1;
        if (numOfAccepts == (alive.size + 1) / 2 + 1) {
          ctx.self ! Decided(ctx.self, proposedValue.get)
          alive.foreach((actorRef, _) => actorRef ! Decided(ctx.self, proposedValue.get))
        }
      }
      Behaviors.same

    case Nack(src, ballot) =>
      //ctx.log.info(s"CRDTActor-$id: received nack from $src, ballot: $ballot, round: $round, id: $id")
      if ((round, id) == ballot) {
        schedulePropose()
      }
      Behaviors.same

    case C_Decide(value) =>
      //ctx.log.info(s"CRDTActor-$id: decided: $value ballot: $acceptedBallot")
      if (value == ctx.self) {
        if(opQueue.nonEmpty) {
          writeCrdt()
          broadcastAndResetDeltas()
        }
        ctx.self ! ReleaseLock
        alive.foreach((actorRef, _) => actorRef ! ReleaseLock)
      }
      Behaviors.same

    case ReleaseLock =>
      resetForNextRound()
      if (opQueue.nonEmpty) schedulePropose()
      Behaviors.same

  Behaviors.same
}