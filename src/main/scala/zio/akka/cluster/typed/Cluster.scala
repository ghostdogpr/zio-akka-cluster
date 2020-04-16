package zio.akka.cluster.typed

import akka.actor.Address
import akka.actor.typed.scaladsl.Behaviors
import akka.actor.typed.{ ActorSystem, Behavior }
import akka.cluster.ClusterEvent.{ ClusterDomainEvent, _ }
import akka.cluster.typed.{ JoinSeedNodes, Leave, Subscribe }
import izumi.reflect.Tags.Tag
import zio.Exit.{ Failure, Success }
import zio.{ Has, Queue, Runtime, Task, ZIO }

object Cluster extends Cluster[Nothing]

abstract class Cluster[T: Tag] {

  private def cluster: ZIO[Has[ActorSystem[T]], Throwable, akka.cluster.typed.Cluster] =
    for {
      actorSystem <- ZIO.access[Has[ActorSystem[T]]](_.get)
      cluster     <- Task(akka.cluster.typed.Cluster(actorSystem))
    } yield cluster

  /**
   *  Returns the current state of the cluster.
   */
  val clusterState: ZIO[Has[ActorSystem[T]], Throwable, CurrentClusterState] =
    for {
      cluster <- cluster
      state   <- Task(cluster.state)
    } yield state

  /**
   *  Joins a cluster using the provided seed nodes.
   */
  def join(seedNodes: List[Address]): ZIO[Has[ActorSystem[T]], Throwable, Unit] =
    for {
      cluster <- cluster
      _       <- Task(cluster.manager ! JoinSeedNodes(seedNodes))
    } yield ()

  /**
   *  Leaves the current cluster.
   */
  val leave: ZIO[Has[ActorSystem[T]], Throwable, Unit] =
    for {
      cluster <- cluster
      _       <- Task(cluster.manager ! Leave(cluster.selfMember.address))
    } yield ()

  /**
   *  Subscribes to the current cluster events. It returns an unbounded queue that will be fed with cluster events.
   *  To unsubscribe, use `queue.shutdown`.
   *  To use a bounded queue, see `clusterEventsWith`.
   */
  val clusterEvents: ZIO[Has[ActorSystem[T]], Throwable, Queue[ClusterDomainEvent]] =
    Queue.unbounded[ClusterDomainEvent].tap(clusterEventsWith)

  /**
   *  Subscribes to the current cluster events, using the provided queue to push the events.
   *  To unsubscribe, use `queue.shutdown`.
   */
  def clusterEventsWith(queue: Queue[ClusterDomainEvent]): ZIO[Has[ActorSystem[T]], Throwable, Unit] =
    for {
      rts         <- Task.runtime
      actorSystem <- ZIO.access[Has[ActorSystem[T]]](_.get[ActorSystem[T]])
      _           <- Task(actorSystem.systemActorOf(SubscriberActor(rts, queue), "SubscriberActor"))
    } yield ()

  private[cluster] object SubscriberActor {
    def apply(rts: Runtime[Any], queue: Queue[ClusterDomainEvent]): Behavior[ClusterDomainEvent] =
      Behaviors.setup { context =>
        val cluster = akka.cluster.typed.Cluster(context.system)
        cluster.subscriptions ! Subscribe(context.self, classOf[ClusterDomainEvent])
        Behaviors.receive { (context, message) =>
          rts.unsafeRunAsync(queue.offer(message)) {
            case Success(_) => ()
            case Failure(cause) =>
              if (cause.interrupted) context.stop(context.self) // stop listening if the queue was shut down
          }
          Behaviors.same
        }
      }
  }
}
