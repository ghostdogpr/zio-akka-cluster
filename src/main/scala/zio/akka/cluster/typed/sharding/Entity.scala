package zio.akka.cluster.typed.sharding

import zio.{ Ref, UIO }

trait Entity[State] {
  def id: String
  def state: Ref[Option[State]]
  def stop: UIO[Unit]
}
