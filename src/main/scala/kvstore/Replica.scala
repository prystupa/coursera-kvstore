package kvstore

import akka.actor._
import kvstore.Arbiter._
import akka.actor.SupervisorStrategy.Escalate
import scala.concurrent.duration._
import java.util.concurrent.TimeUnit
import scala.Some
import akka.actor.OneForOneStrategy

object Replica {

  sealed trait Operation {
    def key: String

    def id: Long
  }

  case class Insert(key: String, value: String, id: Long) extends Operation

  case class Remove(key: String, id: Long) extends Operation

  case class Get(key: String, id: Long) extends Operation

  sealed trait OperationReply

  case class OperationAck(id: Long) extends OperationReply

  case class OperationFailed(id: Long) extends OperationReply

  case class GetResult(key: String, valueOption: Option[String], id: Long) extends OperationReply

  def props(arbiter: ActorRef, persistenceProps: Props): Props = Props(new Replica(arbiter, persistenceProps))
}

class Replica(val arbiter: ActorRef, persistenceProps: Props) extends Actor {

  import Replica._
  import Replicator._
  import Persistence._

  /*
   * The contents of this actor is just a suggestion, you can implement it in any way you like.
   */

  var kv = Map.empty[String, String]
  // a map from secondary replicas to replicators
  var secondaries = Map.empty[ActorRef, ActorRef]
  // the current set of replicators
  var replicators = Set.empty[ActorRef]

  override def preStart(): Unit = {
    arbiter ! Join
  }

  def receive = {
    case JoinedPrimary => context.become(leader)
    case JoinedSecondary => context.become(replica(0L))
  }

  /* TODO Behavior for  the leader role. */
  val leader: Receive = {
    case Insert(key, value, id) => {
      kv = kv updated(key, value)
      persist(key, Some(value), id)
    }

    case Remove(key, id) => {
      kv = kv - key
      persist(key, None, id)
    }

    case Get(key, id) => {
      sender ! GetResult(key, kv.get(key), id)
    }

    case UpdatePersisted(requester, _, _, id) => requester ! OperationAck(id) 

    case _ => ???
  }

  /* TODO Behavior for the replica role. */
  def replica(expectedSeq: Long): Receive = {
    case Snapshot(_, _, seq) if seq > expectedSeq => // Ignore
    case Snapshot(key, _, seq) if seq < expectedSeq => sender ! SnapshotAck(key, seq)
    case snapshot@Snapshot(key, valueOption, seq) => {
      persist(key, valueOption, seq)

      valueOption match {
        case Some(value) => kv = kv updated(key, value)
        case None => kv = kv - key
      }
    }

    case Get(key, id) => {
      sender ! GetResult(key, kv.get(key), id)
    }

    case UpdatePersisted(replicator, key, value, seq) =>
      replicator ! SnapshotAck(key, seq)
      context.become(replica(expectedSeq + 1))

    case _ => ???
  }

  private def persist(key: String, value: Option[String], id: Long): Unit = {
    val requester = sender
    context.actorOf(Props(new Worker(requester, key, value, id)))
  }
  
  class Worker(requester: ActorRef, key: String, value: Option[String], id: Long) extends Actor {

    var persistence: ActorRef = _

    context.setReceiveTimeout(Duration(100, TimeUnit.MILLISECONDS))

    override def supervisorStrategy = OneForOneStrategy() {
      case _: PersistenceException => {
        Escalate
      }
    }

    override def preStart(): Unit = {
      persistence = context.actorOf(persistenceProps)
      persistence ! Persist(key, value, id)
    }

    override def receive = {
      case Persisted(`key`, `id`) => {
        context.parent ! UpdatePersisted(requester, key, value, id)
        context.stop(self)
      }

      case ReceiveTimeout => {
        persistence ! Persist(key, value, id)
      }

      case obj => ???
    }
  }

  private case class UpdatePersisted(requester: ActorRef, key: String, value: Option[String], id: Long)

}
