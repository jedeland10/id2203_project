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
  // State return value for write/read requests of leases
  sealed trait State
  case object Commit extends State
  case object Abort extends State

  // The type of messages that the actor can handle
  sealed trait Command

  // Messages containing the CRDT delta state exchanged between actors
  case class DeltaMsg(from: ActorRef[Command], delta: ReplicatedDelta)
      extends Command

  // Triggers the actor to start the computation (do this only once!)
  case object Start extends Command

  // Triggers the actor to consume an operation (do this repeatedly!)
  case object ConsumeOperation extends Command

  case class Read(from: ActorRef[Command], k: Long) extends Command
  case class Write(from: ActorRef[Command], k: Long, v: mutable.Map[Int, Long]) extends Command

  case class AckRead(from: ActorRef[Command], k: Long, kPrim: Long, v: mutable.Map[Int, Long]) extends Command
  case class NackRead(from: ActorRef[Command], k: Long) extends Command
  
  case class AckWrite(from: ActorRef[Command], k: Long) extends Command
  case class NackWrite(from: ActorRef[Command], k: Long) extends Command
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

  private var read = 0L;
  private var write = 0L;
  private var vi = mutable.Map.empty[Int, Long];


  private val ackReadResponses: mutable.Map[Long, mutable.Map[Int,Long]] = mutable.Map.empty[Long, mutable.Map[Int,Long]];
  private val nackReadResponses = List.empty[Long];
  
  private val ackWriteResponses = List.empty[Long];;
  private val nackWriteResponses = List.empty[Long];;

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
    
  private def read(k: Long): (State, mutable.Map[Int, Long]) = {
    others.foreach {
      (_, actorRef) =>
        actorRef ! Read(ctx.self, k);
    }


    /*
    def handleResponses(ackCount: Int, nackCount: Int, resultMap: mutable.Map[Int, Long]): Unit = {

      if (ackCount >= (others.size + 1) / 2) {
        if (nackCount >= 1) {

        }
      } else if (nackCount >= (others.size + 1) / 2) {

      } else {

      }
    }
    handleResponses(0, 0, mutable.Map.empty)*/

    // Wait until received Ackread or Nackread from (n + 1) / 2 processes

    // if received at least 1 NackRead(k) return (State.Abort, mutable.Map.empty[Int, Long])
    // else select AckRead(k, kPrim, v) with highest kPrim and return (State.Commit, v)
    (Abort, mutable.Map.empty)
  }

        
  
  // This is the event handler of the actor, implement its logic here
  // Note: the current implementation is rather inefficient, you can probably
  // do better by not sending as many delta update messages
  override def onMessage(msg: Command): Behavior[Command] = msg match
    case Start => {
      ctx.log.info(s"CRDTActor-$id started")
      ctx.self ! ConsumeOperation // start consuming operations
      Behaviors.same
    }

    case ConsumeOperation => {
      val key = Utils.randomString()
      val value = Utils.randomInt()
      ctx.log.info(s"CRDTActor-$id: Consuming operation $key -> $value")
      crdtstate = crdtstate.put(selfNode, key, value)
      ctx.log.info(s"CRDTActor-$id: CRDT state: $crdtstate")
      broadcastAndResetDeltas()
      ctx.self ! ConsumeOperation // continue consuming operations, loops sortof
      Behaviors.same
    }

    case DeltaMsg(from, delta) => {
      ctx.log.info(s"CRDTActor-$id: Received delta from ${from.path.name}")
      // Merge the delta into the local CRDT state
      crdtstate = crdtstate.mergeDelta(delta.asInstanceOf) // do you trust me?
      Behaviors.same
    }

    case Read(from, k) => {
      ctx.log.info(s"CRDTActor-$id: Received Read from id ${from.path.name}")
      if (write >= k || read >= k) {
        from ! NackRead(ctx.self, k);
      }
      else{
        read = k;
        from ! AckRead(ctx.self, k, write, vi);
      }
      Behaviors.same
    }

    case Write(from, k, v) => {
      ctx.log.info(s"CRDTActor-$id: Received write from id ${from.path.name}")
      if (write > k || read > k) {
        from ! NackWrite(ctx.self, k);
      }
      else {
        write = k;
        vi = v;
        from ! AckWrite(ctx.self, k);
      }
      Behaviors.same
    }

    case AckRead(from, k, kPrim, v) => {
      ackReadResponses(kPrim) = v;
      Behaviors.same
    }
    case NackRead(from, k) => {
      nackReadResponses.appended(k);
      Behaviors.same
    }
    case AckWrite(from, k) => {
      ackWriteResponses.appended(k);
      Behaviors.same
    }
    case NackWrite(from, k) => {
      nackWriteResponses.appended(k)
      Behaviors.same
    }

  Behaviors.same
}
