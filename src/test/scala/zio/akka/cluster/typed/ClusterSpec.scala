package zio.akka.cluster.typed

import akka.actor.typed.ActorSystem
import akka.actor.typed.scaladsl.Behaviors
import akka.cluster.ClusterEvent.MemberUp
import com.typesafe.config.{ Config, ConfigFactory }
import zio.test.Assertion._
import zio.test._
import zio.test.environment.TestEnvironment
import zio.{ Managed, Task, ZLayer }

object ClusterSpec extends DefaultRunnableSpec {

  def spec: ZSpec[TestEnvironment, Any] =
    suite("ClusterSpec")(
      testM("receive cluster events") {
        val config: Config = ConfigFactory.parseString(s"""
                                                          |akka {
                                                          |  actor {
                                                          |    provider = "cluster"
                                                          |  }
                                                          |  remote {
                                                          |    artery.canonical {
                                                          |      hostname = "127.0.0.1"
                                                          |      port = 2551
                                                          |    }
                                                          |  }
                                                          |  cluster {
                                                          |    seed-nodes = ["akka://Test@127.0.0.1:2551"]
                                                          |  }
                                                          |}
                  """.stripMargin)

        val actorSystem: Managed[Throwable, ActorSystem[Nothing]] =
          Managed.make(Task(ActorSystem[Nothing](Behaviors.ignore[Nothing], "Test", config)))(sys =>
            Task.fromFuture { _ =>
              sys.terminate()
              sys.whenTerminated
            }.either
          )

        assertM(
          for {
            queue <- Cluster.clusterEvents
            _     <- Cluster.leave
            item  <- queue.take
          } yield item
        )(isSubtype[MemberUp](anything)).provideLayer(ZLayer.fromManaged(actorSystem))
      }
    )
}
