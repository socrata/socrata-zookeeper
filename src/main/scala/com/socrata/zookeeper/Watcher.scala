 package com.socrata.zookeeper

import org.apache.{zookeeper => zk}

abstract class Watcher extends zk.Watcher {
  final def process(event: zk.WatchedEvent) {
    process(WatchedEvent(event))
  }

  def process(event: WatchedEvent)
}

sealed abstract class WatchedEvent
object WatchedEvent {
  def apply(event: zk.WatchedEvent) = event.getType match {
    case zk.Watcher.Event.EventType.None =>
      ConnectionStateChanged(ConnectionState(event.getState))
    case zk.Watcher.Event.EventType.NodeDeleted =>
      NodeDeleted(event.getPath)
    case zk.Watcher.Event.EventType.NodeDataChanged =>
      NodeDataChanged(event.getPath)
    case zk.Watcher.Event.EventType.NodeCreated =>
      NodeDataChanged(event.getPath)
    case zk.Watcher.Event.EventType.NodeChildrenChanged =>
      NodeChildrenChanged(event.getPath)
  }
}

sealed trait ExistsEvent
sealed trait ReadEvent
sealed trait ChildrenEvent

case class ConnectionStateChanged(newState: ConnectionState) extends WatchedEvent with ExistsEvent with ReadEvent with ChildrenEvent
sealed abstract class NodeEvent extends WatchedEvent {
  def path: String
}
object NodeEvent {
  def unapply(x: NodeEvent) = Some(x.path)
}
case class NodeDeleted(path: String) extends NodeEvent with ExistsEvent with ReadEvent with ChildrenEvent
case class NodeDataChanged(path: String) extends NodeEvent with ExistsEvent with ReadEvent
case class NodeCreated(path: String) extends NodeEvent with ExistsEvent
case class NodeChildrenChanged(path: String) extends NodeEvent with ChildrenEvent

sealed abstract class ConnectionState
object ConnectionState {
  def apply(state: zk.Watcher.Event.KeeperState) = state match {
    case zk.Watcher.Event.KeeperState.Disconnected => Disconnected
    case zk.Watcher.Event.KeeperState.Expired => Expired
    case zk.Watcher.Event.KeeperState.SyncConnected => Connected
  }
}

case object Disconnected extends ConnectionState
case object Expired extends ConnectionState
case object Connected extends ConnectionState

