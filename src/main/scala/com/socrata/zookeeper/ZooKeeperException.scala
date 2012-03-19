package com.socrata.zookeeper

import java.io.IOException

class ZooKeeperException(msg: String, cause: Throwable) extends RuntimeException(msg, cause) {
  def this(msg: String) = this(msg, null)
  def this(cause: Throwable) = this(null, cause)
}

case class ZooKeeperConnectionException(cause: IOException) extends ZooKeeperException(cause)

case class ZooKeeperLogicError(msg: String) extends ZooKeeperException(msg)
