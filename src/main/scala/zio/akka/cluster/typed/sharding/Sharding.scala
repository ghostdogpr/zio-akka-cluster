package zio.akka.cluster.typed.sharding

import akka.actor.typed.scaladsl.Behaviors
import akka.actor.typed.{ ActorRef, ActorSystem, Behavior }
import akka.cluster.sharding.typed.ShardingEnvelope
import akka.cluster.sharding.typed.scaladsl.{ ClusterSharding, EntityContext, EntityTypeKey, Entity => E }
import izumi.reflect.Tags.Tag
import zio.{ Has, Ref, Runtime, Task, UIO, ZIO }

import scala.reflect.ClassTag

/**
 *  A `Sharding[M]` is able to send messages of type `M` to a sharded entity or to stop one.
 */
trait Sharding[M] {

  def send(entityId: String, data: M): Task[Unit]

  def stop(entityId: String): Task[Unit]

}

object Sharding {

  /**
   *  Starts cluster sharding on this node for a given entity type.
   *
   * @param name      the name of the entity type
   * @param onMessage the behavior of the entity when it receives a message
   * @param role      an optional role to specify that this entity type is located on cluster nodes with a specific role
//   * @param askTimeout     a finite duration specifying how long an ask is allowed to wait for an entity to respond
   * @return a [[Sharding]] object that can be used to send messages to sharded entities
   */
  def start[T: Tag, Msg: ClassTag, State](
    name: String,
    onMessage: Msg => ZIO[Entity[State], Nothing, Unit],
    role: Option[String] = None
//    askTimeout: FiniteDuration = 10.seconds
  ): ZIO[Has[ActorSystem[T]], Throwable, Sharding[Msg]] =
    for {
      rts            <- ZIO.runtime[Has[ActorSystem[T]]]
      actorSystem    <- ZIO.access[Has[ActorSystem[T]]](_.get[ActorSystem[T]])
      sharding       <- Task(ClusterSharding(actorSystem))
      entity         = E(EntityTypeKey[Msg](name))(makeBehavior(_, rts)(onMessage))
      entityWithRole = role.fold(entity)(role => entity.withRole(role))
      shardingRegion <- Task(sharding.init(entityWithRole))
    } yield new ShardingImpl[Msg] {
      override val getShardingRegion: ActorRef[ShardingEnvelope[Msg]] = shardingRegion
    }

  private[sharding] trait ShardingImpl[Msg] extends Sharding[Msg] {
    val getShardingRegion: ActorRef[ShardingEnvelope[Msg]]

    override def send(entityId: String, data: Msg): Task[Unit] =
      Task(getShardingRegion ! ShardingEnvelope(entityId, data))

    override def stop(entityId: String): Task[Unit] = ???
//      Task(getShardingRegion ! ShardingEnvelope(entityId, Passivate))
  }

  private[sharding] def makeBehavior[Msg, State](entityContext: EntityContext[Msg], rts: Runtime[Any])(
    onMessage: Msg => ZIO[Entity[State], Nothing, Unit]
  ): Behavior[Msg] =
    Behaviors.setup { context =>
      val ref: Ref[Option[State]] = rts.unsafeRun(Ref.make[Option[State]](None))
      val entity: Entity[State] = new Entity[State] {
        override def id: String                = entityContext.entityId
        override def state: Ref[Option[State]] = ref
        override def stop: UIO[Unit]           = UIO(context.stop(context.self))
      }

      Behaviors.receiveMessage[Msg] { msg =>
        rts.unsafeRunSync(onMessage(msg).provide(entity))
        Behaviors.same
      }
    }
}
