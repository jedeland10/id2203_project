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

// using ? to send message, ask
import org.apache.pekko.actor.typed.scaladsl.AskPattern.Askable
import org.apache.pekko.util.Timeout
import scala.concurrent.duration._

object CRDTActor {
  // State return value for write/read requests of leases
  sealed trait State
  case object Commit extends State
  case object Abort extends State
  case object Unknown extends State

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

  private var read = 0L; // Latest read timestamp
  private var write = 0L; // Latest write timestamp
  private var vi = mutable.Map.empty[Int, Long]; // Latest lease process coupled with timestamp


  private val ackReadResponses: mutable.Map[Long, mutable.Map[Int,Long]] = mutable.Map.empty[Long, mutable.Map[Int,Long]];
  private val nackReadResponses: mutable.Map[ActorRef[Command],Long] = mutable.Map.empty[ActorRef[Command], Long];
  
  private val ackWriteResponses: mutable.Map[ActorRef[Command],Long] = mutable.Map.empty[ActorRef[Command], Long];
  private val nackWriteResponses: mutable.Map[ActorRef[Command],Long] = mutable.Map.empty[ActorRef[Command], Long];

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
    
  private def read(k: Long) = {
    ctx.log.info(s"CRDTActor-$id wants to read with k = $k")

    others.foreach {
      (_, actorRef) => {
        actorRef ! Read(ctx.self, k)
      }
    }
  }

  private def requestWrite(k: Long, v: mutable.Map[Int, Long]) = {
    ctx.log.info(s"CRDTActor-$id wants to write $v")
    val key = Utils.randomString()
    val value = Utils.randomInt()

    ctx.log.info(s"CRDTActor-$id: Consuming operation $key -> $value")
    crdtstate = crdtstate.put(selfNode, key, value)
    ctx.log.info(s"CRDTActor-$id: CRDT state: $crdtstate")
    broadcastAndResetDeltas()

    ctx.self ! ConsumeOperation // continue consuming operations, loops sortof
  }

    // Wait until received Ackread or Nackread from (n + 1) / 2 processes

    // if received at least 1 NackRead(k) return (State.Abort, mutable.Map.empty[Int, Long])
    // else select AckRead(k, kPrim, v) with highest kPrim and return (State.Commit, v)

  
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
      //val key = Utils.randomString()
      //val value = Utils.randomInt()
      //ctx.log.info(s"CRDTActor-$id: Consuming operation $key -> $value")
      //crdtstate = crdtstate.put(selfNode, key, value)
      //ctx.log.info(s"CRDTActor-$id: CRDT state: $crdtstate")
      //broadcastAndResetDeltas()
      read(System.currentTimeMillis())
      //ctx.self ! ConsumeOperation // continue consuming operations, loops sortof
      Behaviors.same
    }

    case DeltaMsg(from, delta) => {
      ctx.log.info(s"CRDTActor-$id: Received delta from ${from.path.name}")
      // Merge the delta into the local CRDT state
      crdtstate = crdtstate.mergeDelta(delta.asInstanceOf) // do you trust me?
      ackReadResponses.clear()
      Behaviors.same
    }

    case Read(from, k) => {
      ctx.log.info(s"CRDTActor-$id: Received Read from id ${from.path.name}")
      if (write >= k || read >= k) {
        from ! NackRead(ctx.self, k);
      }
      else {
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
      ctx.log.info(s"CRDTActor-$id: Received ACKREAD from id ${from.path.name}")
      ackReadResponses(kPrim) = v;
      ctx.log.info(s"CRDTActor-$id: ACKREADS: ${ackReadResponses}")
      if (ackReadResponses.size >= (others.size + 1) / 2 && !(nackReadResponses.size == 0)) {
        val highestKprim = ackReadResponses.maxBy(_._1)
        ctx.log.info(s"highest: $highestKprim")
        //requestWrite()
      }
      Behaviors.same
    }
    case NackRead(from, k) => {
      ctx.log.info(s"CRDTActor-$id: Received NACKREAD from id ${from.path.name}")
      nackReadResponses(from) = k;
      ctx.log.info(s"CRDTActor-$id: NACKREADs: ${nackReadResponses}")
      Behaviors.same
    }
    case AckWrite(from, k) => {
      ctx.log.info(s"CRDTActor-$id: Received ACKWRITE from id ${from.path.name}")
      ackWriteResponses(from) = k;
      ctx.log.info(s"CRDTActor-$id: ACKWRITEs: ${ackWriteResponses}")
      Behaviors.same
    }
    case NackWrite(from, k) => {
      ctx.log.info(s"CRDTActor-$id: Received NACKWRITE from id ${from.path.name}")
      nackWriteResponses(from) = k;
      ctx.log.info(s"CRDTActor-$id: NACKWRITEs: ${nackWriteResponses}")
      Behaviors.same
    }

  Behaviors.same
}
