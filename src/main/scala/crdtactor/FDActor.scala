//package crdtactor
//
//case class Heartbeat(sender: ActorRef)
//
//class FDActor extends Actor[Any] {
//  private var lastSeen: Map[ActorRef, Long] = Map.empty
//
//  override def receive: Receive = {
//    case msg: Heartbeat =>
//      lastSeen += (msg.sender -> System.currentTimeMillis())
//    case _ => // Ignore other messages
//  }
//
//  def isAlive(ref: ActorRef, timeout: Long): Boolean = {
//    lastSeen.get(ref).fold(false)(t => System.currentTimeMillis() - t <= timeout)
//  }
//}
