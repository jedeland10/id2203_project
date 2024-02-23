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
import collection.mutable

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

  case class RequestLocks(sender: ActorRef[Command]) extends Command
  case class GrantLock(key: ActorRef[Command], value: Boolean) extends Command
  case class ReleaseLocks() extends Command
  case class ReturnLock() extends Command
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

  //private var localLock: Map[String, Boolean] = Map("hasLock" -> true)
  private var localLock: Boolean = true
  private val lockMap: mutable.Map[ActorRef[Command], Boolean] = mutable.Map.empty[ActorRef[Command], Boolean]
  
  // This is the event handler of the actor, implement its logic here
  // Note: the current implementation is rather inefficient, you can probably
  // do better by not sending as many delta update messages
  override def onMessage(msg: Command): Behavior[Command] = msg match
    case Start =>
      ctx.log.info(s"CRDTActor-$id started")
      ctx.self ! ConsumeOperation // start consuming operations
      Behaviors.same

    case ConsumeOperation =>
      val key = Utils.randomString()
      val value = Utils.randomInt()
      ctx.log.info(s"CRDTActor-$id: Consuming operation $key -> $value")

      crdtstate = crdtstate.put(selfNode, key, value)
      ctx.log.info(s"CRDTActor-$id: CRDT state: $crdtstate")
      broadcastAndResetDeltas()
      //Start by locking so no other processor can execute the same part of the code
      //Sync CRDTs state with all other processors
      if (localLock) {
        others.foreach { //Filter in others so that nodes in lockMap is removed as well as self!
            (name, actorRef) =>
              actorRef !
                RequestLocks(ctx.self)
          }
        //Create a sync function!!!
        ctx.log.info(s"CRDTActor-$id: Size of others: ${others.size}")
        ctx.log.info(s"CRDTActor-$id: Size of lockMap: ${lockMap.size}")
        if (lockMap.size == others.size) {
          ctx.log.info(s"CRDTActor-$id: I have the lock!")
          val key = Utils.randomString()
          val value = Utils.randomInt()
          ctx.log.info(s"CRDTActor-$id: Consuming operation $key -> $value")
          crdtstate = crdtstate.put(selfNode, key, value)
          ctx.log.info(s"CRDTActor-$id: CRDT state: $crdtstate")
          broadcastAndResetDeltas()
          ctx.log.info(s"CRDTActor-$id: I now release the locks")
          lockMap.clear()
        }
        others.foreach {
          (name, actorRef) =>
            actorRef !
              ReturnLock()
        }
      }
      ctx.self ! ConsumeOperation // continue consuming operations, loops sortof
      Behaviors.same

    case DeltaMsg(from, delta) =>
      ctx.log.info(s"CRDTActor-$id: Received delta from ${from.path.name}")
      // Merge the delta into the local CRDT state
      crdtstate = crdtstate.mergeDelta(delta.asInstanceOf) // do you trust me?
      Behaviors.same

    case RequestLocks(sender) =>
      //ctx.log.info(s"CRDTActor-$id: Someone wants my lock!")
      //If I have my lock, give it away otherwise send NO
      if (localLock && lockMap.size == 0) { //Filter so no true val is in map
        sender ! GrantLock(ctx.self, true)
        //TODO SEND STATE ALSO
        localLock = false;
      } else {
        sender ! GrantLock(ctx.self, false)
      }
      Behaviors.same

    case GrantLock(key, value) => //this is exceuted in the requester
      //ADDING TO lockMap doesnt work
      lockMap(key) = value
      //lockMap = lockMap + (key -> value)
      //localLock = true;
      //localLock = localLock.updated("hasLock", false)
      //If we have a map with the length of the number of processors and all returend true, we have the lock and can execute
      Behaviors.same

    // case ReleaseLocks() =>
    //   //iterate over lockMap and send a message to all agents in the map
    //   for (agents <- lockMap)
    //     agents._1 ! ReturnLock()
    //     lockMap - agents._2
    //   Behaviors.same

    case ReturnLock() =>
      localLock = true;
      Behaviors.same
      //localLock = localLock.updated("hasLock", true)

  Behaviors.same
}
