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

  private def readFunc(k: Int): Unit = {
    ackVotes = 0
    nackVotes = 0
    highestK = 0
    others.foreach {
          (name, actorRef) =>
          if (actorRef != ctx.self) {
            actorRef ! Read(k, ctx.self)
          }
        }
  }

  private def writeFunc(k: Int, v: (Int,Long)): Unit = {
    ackVotes = 0
    nackVotes = 0
    others.foreach {
          (name, actorRef) =>
          if (actorRef != ctx.self) {
            actorRef ! Write(k, v, ctx.self)
          }
        }
  }

  //This is my getLease function
  private def commit(v: (Int, Long)): Unit = {
    ctx.log.info(s"CRDTActor-$id: Inside the commit func")
    if (v != null) {
      if (v._2 < System.currentTimeMillis() && (v._2 + 500) > System.currentTimeMillis()) {
        ki = ki + 1
        Thread.sleep(500)
        ctx.self ! ConsumeOperation // start consuming operations
        Behaviors.same
      }
    }
    if (v == null) { //Add check for time now later
      writeFunc(ki, (ki,System.currentTimeMillis() + 1000))
    } else if (v._2 < System.currentTimeMillis()) {
      println("v._2: " + v._2)
      println("v._1: " + v._1)
      println("v: " + v)
      writeFunc(ki, (ki,System.currentTimeMillis() + 1000))
    } else if (v._1 == ki) {
      writeFunc(ki, (ki,System.currentTimeMillis() + 1000))
    }

      // ctx.log.info(s"CRDTActor-$id: acks: $ackVotes - nacks: $nackVotes")
      // val key = Utils.randomString()
      // val value = Utils.randomInt()
      // ctx.log.info(s"CRDTActor-$id: Consuming operation $key -> $value")
      // crdtstate = crdtstate.put(selfNode, key, value)
      // ctx.log.info(s"CRDTActor-$id: CRDT state: $crdtstate")
      // broadcastAndResetDeltas()
      //ctx.self ! ConsumeOperation // continue consuming operations, loops sortof
    Behaviors.same
  }

  private def writeCommit(): Unit = {
    //SYNC then do this
    //ctx.log.info(s"CRDTActor-$id: acks: $ackVotes - nacks: $nackVotes")
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
      ctx.self ! ConsumeOperation // start consuming operations
      Behaviors.same

    case ConsumeOperation =>
      readFunc(ki)
      Behaviors.same
    

    case DeltaMsg(from, delta) =>
      //ctx.log.info(s"CRDTActor-$id: Received delta from ${from.path.name}")
      // Merge the delta into the local CRDT state
      crdtstate = crdtstate.mergeDelta(delta.asInstanceOf) // do you trust me?
      ctx.log.info(s"CRDTActor-$id: CRDT state: $crdtstate")
      Behaviors.same
    
    case Read(k, sender) =>
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
      nackVotes = nackVotes + 1
      if (nackVotes > (others.size/2)) {
        //abort
        ctx.self ! ConsumeOperation // start consuming operations
        Behaviors.same
      }
      Behaviors.same
    

    case AckRead(k, writej, v) => 
      ackVotes = ackVotes + 1
      if (writej > highestK) {
        highestK = writej
        tempV = v
      }
      if (ackVotes > (others.size/2) && nackVotes == 0) {
        commit(tempV)
        // ctx.self ! ConsumeOperation // continue consuming operations, loops sortof
        // Behaviors.same
      } else if(ackVotes > (others.size/2) && nackVotes > 0) {
        ctx.self ! ConsumeOperation // continue consuming operations, loops sortof
        Behaviors.same
      }
      Behaviors.same
    

    case NackWrite(k) => 
      nackVotes = nackVotes + 1
      if (nackVotes > (others.size/2)) {
        //abort
        ctx.self ! ConsumeOperation // start consuming operations
        Behaviors.same
      }
      Behaviors.same

    case AckWrite(k) => 
      ackVotes = ackVotes + 1
      if (ackVotes > (others.size/2) && nackVotes == 0) {
        //WRITE IS DONE LETSS GO
        //CALL ON LETS GO FUNCTION
        writeCommit()
      } else if(ackVotes > (others.size/2) && nackVotes > 0) {
        //Restart
        ctx.self ! ConsumeOperation // continue consuming operations, loops sortof
        Behaviors.same
      }
      Behaviors.same

  Behaviors.same
}
