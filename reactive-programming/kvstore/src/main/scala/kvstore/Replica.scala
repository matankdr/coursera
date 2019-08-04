package kvstore

import akka.actor.{Actor, ActorLogging, ActorRef, OneForOneStrategy, PoisonPill, Props, SupervisorStrategy, Terminated}
import kvstore.Arbiter._

import scala.collection.immutable.Queue
import akka.actor.SupervisorStrategy.Restart

import scala.annotation.tailrec
import akka.pattern.{ask, pipe}

import scala.concurrent.duration._
import akka.util.Timeout

import scala.concurrent.Future

object Replica {
  sealed trait Operation {
    def key: String
    def id: Long
  }
  case class Insert(key: String, value: String, id: Long) extends Operation
  case class Remove(key: String, id: Long) extends Operation
  case class Get(key: String, id: Long) extends Operation

  case object RetryPersist
  case class AckTimeout(id: Long)

  sealed trait OperationReply
  case class OperationAck(id: Long) extends OperationReply
  case class OperationFailed(id: Long) extends OperationReply
  case class GetResult(key: String, valueOption: Option[String], id: Long) extends OperationReply

  def props(arbiter: ActorRef, persistenceProps: Props): Props = Props(new Replica(arbiter, persistenceProps))
}

class Replica(val arbiter: ActorRef, persistenceProps: Props) extends Actor with ActorLogging{
  import Replica._
  import Replicator._
  import Persistence._
  import context.dispatcher

  /*
   * The contents of this actor is just a suggestion, you can implement it in any way you like.
   */

  var kv = Map.empty[String, String]
  // a map from secondary replicas to replicators
  var secondaries = Map.empty[ActorRef, ActorRef]
  // the current set of replicators
  var replicators = Set.empty[ActorRef]

  // persist message that not acked yet
  case class Ack(requester: ActorRef, persistMsg: Option[Persist], replicatedBy: Set[ActorRef])
  var notAcked = Map.empty[Long, Ack]

  var expectedSeq = 0L
  var persistor: ActorRef = context.actorOf(persistenceProps)

  override def preStart(): Unit = {
    arbiter ! Join
    context.system.scheduler.schedule(100 millis, 100 millis, self, RetryPersist)
  }

  def receive = {
    case JoinedPrimary   => context.become(leader)
    case JoinedSecondary => context.become(replica)
  }


//  case class ReplicationAck(key: String, requester: Option[ActorRef], pending: Int)
//  type ReplicationAcks = Map[Long, ReplicationAck]

  def replicate(key: String, value: Option[String], id: Long)(implicit timeout: Timeout): Future[Set[Replicated]] = {
    val replicateMsg = Replicate(key, value, id)
    val responsesFromReplicators = replicators.map(_.ask(replicateMsg).mapTo[Replicated])

    Future.sequence(responsesFromReplicators)
  }

  /* TODO Behavior for  the leader role. */
  val leader: Receive = {
    case Insert(key, value, id) =>
      log.info(s"insert [$id]: $key -> $value")
      kv += (key -> value)

      replicators.foreach(_  ! Replicate(key, Some(value), id))
      persist(id, key, Some(value), sender)
      context.system.scheduler.scheduleOnce(1000 millis, self, AckTimeout(id))

//      log.info(s"replicate insert [$id] on ${secondaries.keys}")
//      replicators.foreach(_  ! Replicate(key, Some(value), id))

    case Remove(key, id) =>
      kv -= key



//      val replicationResponse = replicate(key, None, id)
//        .map (_ => requester ! OperationAck(id))

//      requester ! OperationAck(id)
      log.info(s"persist insert [$id]")
      persist(id, key, None, sender)
      context.system.scheduler.scheduleOnce(1000 millis, self, AckTimeout(id))

      log.info(s"replicate remove [$id] on ${secondaries.keys}")
      replicators.foreach(_  ! Replicate(key, None, id))

//      log.info(s"persist remove [$id]")
//      val persistenceResponse = (persistor ? Persist(key, None, id)).mapTo[Persisted]

//      Future.sequence(Seq(replicationResponse, persistenceResponse))
//        .recover{ case _ => requester ! OperationFailed(id) }

    case Persisted(_, id) =>
      notAcked.get(id).collect {
        case ack if ack.persistMsg.nonEmpty =>
          notAcked += id -> ack.copy(persistMsg = None)
          checkAckStatus(id)
//        requester ! OperationAck(id)
      }

//      notAcked -= id

    case Replicated(_, id) =>
      notAcked.get(id).foreach { ack =>
        notAcked += id -> ack.copy(replicatedBy = ack.replicatedBy + sender())
        checkAckStatus(id)
      }

    case AckTimeout(id) =>
      checkAckStatus(id)
      notAcked.get(id).foreach { case Ack(requester, _, _) =>
          requester ! OperationFailed(id)
          notAcked -= id
      }

    case Get(key, id) =>
      sender ! GetResult(key, kv.get(key), id)

    case Replicas(replicas) =>
      var updatedReplicators = Set.empty[ActorRef]
      var updatedSecondaries = Map.empty[ActorRef, ActorRef]

      (replicas - self) map { replica =>

        val replicator = secondaries.getOrElse(replica, context.actorOf(Replicator.props(replica)))

        if (!secondaries.contains(replica)) {
          kv.zipWithIndex foreach { case ((k,v), index) => replicator ! Replicate(k, Some(v), index) }
        }

        updatedReplicators += replicator
        replicators -= replicator  //Remove active from original set
        updatedSecondaries += (replica -> replicator)
      }

      replicators.foreach(context stop _)
      replicators = updatedReplicators
      secondaries = updatedSecondaries
      notAcked.keySet.foreach(checkAckStatus)

    case RetryPersist =>
      notAcked.values foreach { case Ack(_, persistMessage, _) =>
        persistMessage.foreach(persistor ! _)
      }
  }

  /* TODO Behavior for the replica role. */
  val replica: Receive = {
    case Get(key, id) =>
      val result = kv.get(key)
      sender ! GetResult(key, result, id)

    case Snapshot(key, valueOption, seq) if seq == expectedSeq =>
      valueOption match {
        case Some(value) => kv += (key -> value)
        case None        => kv -= key
      }

      persist(seq, key, valueOption, sender)
//      persistor ! Persist(key, valueOption, seq)
//      sender ! SnapshotAck(key, seq)

    case Snapshot(key, _, seq) if seq < expectedSeq =>
      sender ! SnapshotAck(key, seq)

    case Persisted(key, id) =>
      notAcked.get(id).foreach { case Ack(requester, Some(persistMsg), _) =>
        requester ! SnapshotAck(key, id)
        expectedSeq += 1
      }

      notAcked -= id

    case RetryPersist =>
      notAcked.values foreach { case Ack(_, Some(persistMessage), _) =>
        persistor ! persistMessage
      }
  }

  private def checkAckStatus(id: Long) = {
    notAcked.get(id).collect {
      case Ack(requester, None, replicatedBy) if replicators.forall(replicatedBy.contains) =>
        requester ! OperationAck(id)
        notAcked -= id
    }
  }

  private def persist(id: Long, key: String, valueOption: Option[String], requester: ActorRef) = {
    val persistMessage = Persist(key, valueOption, id)
    notAcked += id -> Ack(requester, Some(persistMessage), Set.empty[ActorRef])

    if (valueOption.isEmpty) {
      log.info(s"persist remove [$id]")
    } else {
      log.info(s"persist insert [$id]")
    }

    persistor ! persistMessage
  }

}

