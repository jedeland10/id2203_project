package crdtactor

import org.apache.pekko.actor.typed.scaladsl.ActorContext
import org.apache.pekko.actor.typed.scaladsl.AbstractBehavior
import org.apache.pekko.actor.typed.scaladsl.Behaviors
import org.apache.pekko.actor.typed.Behavior
import org.apache.pekko.cluster.ddata
import org.apache.pekko.cluster.ddata.DeltaReplicatedData
import org.apache.pekko.cluster.ddata.ReplicatedData
import org.apache.pekko.cluster.ddata.ReplicatedDelta
import org.apache.pekko.cluster.ddata.SelfUniqueAddress
import org.apache.pekko.actor.typed.ActorRef
import scala.collection.mutable.Queue

object CRDTActorLocks {
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
  private var lockQueue = Queue.empty[ActorRef[Command]] //Queue of actors waiting for the lock
  private var mapOfLocks = Map.empty[ActorRef[Command], Boolean] //Map of actors and their locks
  var keyToBe = "" //Key to be added to the CRDT
  var valueToBe = 0 //Value to be added to the CRDT
  private var atomicMap = Map.empty[String, Int] //Array of key-value pairs to be added to the CRDT

  // Hack to get the actor references of the other actors, check out `lazy val`
  // Careful: make sure you know what you are doing if you are editing this code
  private lazy val others =
  Utils.GLOBAL_STATE.getAll[Int, ActorRef[Command]]()

  // Note: you probably want to modify this method to be more efficient
  private def broadcastAndResetDeltas(): Unit =
    val deltaOption = crdtstate.delta
    deltaOption match
      case None => ()
      case Some(delta) =>
        crdtstate = crdtstate.resetDelta // May be omitted
        others.foreach { //
          (name, actorRef) =>
            actorRef !
              DeltaMsg(ctx.self, delta)
        }

  // This is the event handler of the actor, implement its logic here
  // Note: the current implementation is rather inefficient, you can probably
  // do better by not sending as many delta update messages
  override def onMessage(msg: Command): Behavior[Command] = msg match
    case Start =>
      ctx.log.info(s"CRDTActor-$id started")
      ctx.self ! ConsumeOperation // start consuming operations
      Behaviors.same

    case ConsumeOperation =>
      ctx.self ! ConsumeOperation // continue consuming operations, loops sortof
      Behaviors.same

    case DeltaMsg(from, delta) =>
      //ctx.log.info(s"CRDTActor-$id: Received delta from ${from.path.name}")
      // Merge the delta into the local CRDT state
      crdtstate = crdtstate.mergeDelta(delta.asInstanceOf) // do you trust me?
      Behaviors.same

    case Put(key, value) => //Tries to add a value into the CRDT
      keyToBe = key
      valueToBe = value
      // Request locks from all other actors
      others.foreach { (_, actorRef) =>
        actorRef ! AcquireLock(ctx.self)
      }
      Behaviors.same

    case Get(replyTo) =>
      replyTo ! responseMsg(crdtstate) //Send the state to the requester
      Behaviors.same

    case AcquireLock(requester) => //Send lock to requester if you have it, otherwise enuqueu it
      //ctx.log.info(s"CRDTActor-$id: Someone wants my lock")
//      println("Ctx.self: " + ctx.self)
//      println("SelfNode: " + selfNode)
      if (requester != ctx.self) {
        if (hasLock) {
          ctx.log.info(s"CRDTActor-$id: Will try to send lock to $requester")
          requester ! LockAcquired(ctx.self) //Give the lock to the requester
          hasLock = false
        } else {
          // Add requester to the queue, because someone else has the lock
          //ctx.log.info(s"CRDTActor-$id: I don't have the lock, so I will enqueue it: $lockQueue")
          lockQueue = lockQueue.enqueue(requester)
          ctx.log.info(s"CRDTActor-$id: I don't have the lock, so I will enqueue it: $lockQueue")
        }
      }
      Behaviors.same

    case LockAcquired(sender) => //receive a lock from another actor
      //save key and value to CRDT and Broadcast
      //Check that you have all the locks, if so, add to CRDT, broadcast and release the locks
      //ctx.log.info(s"CRDTActor-$id: I got a lock!")
      mapOfLocks = mapOfLocks + (sender -> true)
      ctx.log.info(s"CRDTActor-$id: I got a lock from $sender and the size of the map is ${mapOfLocks.size} and the size of others is ${others.size-1}")
      if (mapOfLocks.size == others.size-1) {
        ctx.log.info(s"CRDTActor-$id: I have all the locks!")
        if (atomicMap.nonEmpty) {
          //Foreach value and key in the atomicArray, add it to the CRDT
          ctx.log.info(s"CRDTActor-$id: I will add the atomicMap to the CRDT")
          atomicMap.foreach { (key, value) =>
            crdtstate = crdtstate.put(selfNode, key, value)
          }
          atomicMap = Map.empty[String, Int]
        } else {
          crdtstate = crdtstate.put(selfNode, keyToBe, valueToBe)
        }
        broadcastAndResetDeltas()
        mapOfLocks.foreach { (actorRef, _) =>
          actorRef ! LockReleased
        }
        mapOfLocks = Map.empty[ActorRef[Command], Boolean]
        hasLock = true //Possibly redundant, but don't have time to check
      } else {
        //wait for more
      }
      Behaviors.same

    case LockReleased => //release a lock to another actor
      //Check if there are more actors in the queue, if so, send the lock to the next actor
      ctx.log.info(s"CRDTActor-$id: I got my lock back!")
      hasLock = true
      if (lockQueue.nonEmpty) {
        //if im the first one in the queue, ask for the locks of the others
        val actorRef = lockQueue.dequeue
        actorRef ! LockAcquired(actorRef)

        //actorRef !
        /*if (actorRef == ctx.self) {
          others.foreach { (_, actorRef) =>
            actorRef ! AcquireLock(ctx.self)
          }
        } else {
          //do nothing
        }*/
      }
      Behaviors.same

    //Should take a an array of keys and values
    case AtomicPut(mapInput) =>
      //foreach pair of keys and values inside the parameter array, add the key and value to the CRDT
      atomicMap = mapInput
      others.foreach { (_, actorRef) =>
        actorRef ! AcquireLock(ctx.self)
      }
      Behaviors.same

  Behaviors.same
}