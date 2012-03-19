package com.socrata.zookeeper

import org.apache.{zookeeper => zk}

package results {
  object Exists {
    sealed trait Result
    case class OK(s: Option[zk.data.Stat]) extends Result
  }

  object Create {
    sealed trait Result
    case object OK extends Result
  }

  object CreateWithCounter {
    sealed trait Result
    case class OK(path: String) extends Result
  }

  object Children {
    sealed trait Result
    case class OK(children: Set[String], stat: zk.data.Stat) extends Result
  }

  object Read {
    sealed trait Result
    case class OK(data: Array[Byte], stat: zk.data.Stat) extends Result
  }

  object Write {
    sealed trait Result
    case class OK(stat: zk.data.Stat) extends Result
  }

  object WriteAnyVersion {
    sealed trait Result
    case class OK(stat: zk.data.Stat) extends Result
  }

  object Delete {
    sealed trait Result
    case object OK extends Result
  }

  object DeleteAnyVersion {
    sealed trait Result
    case object OK extends Result
  }

  case object NotFound extends Children.Result with Read.Result with Write.Result with WriteAnyVersion.Result with Delete.Result with DeleteAnyVersion.Result
  case object NoPath extends Create.Result with CreateWithCounter.Result
  case object AlreadyExists extends Create.Result
  case object BadVersion extends Write.Result with Delete.Result
  case object NotEmpty extends Delete.Result with DeleteAnyVersion.Result

  case object ConnectionLost extends Exists.Result with Create.Result with CreateWithCounter.Result with Children.Result with Read.Result with Write.Result with WriteAnyVersion.Result with Delete.Result with DeleteAnyVersion.Result
  case object SessionExpired extends Exists.Result with Create.Result with CreateWithCounter.Result with Children.Result with Read.Result with Write.Result with WriteAnyVersion.Result with Delete.Result with DeleteAnyVersion.Result
}
