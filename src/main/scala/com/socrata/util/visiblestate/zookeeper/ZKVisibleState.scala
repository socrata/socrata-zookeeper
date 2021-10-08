package com.socrata.util.visiblestate.zookeeper

import com.socrata.zookeeper.ZooKeeperProvider
import com.socrata.util.visiblestate.VisibleState
import annotation.tailrec
import com.socrata.zookeeper.results._

class ZKVisibleState(zkp: ZooKeeperProvider, name: String) extends VisibleState {
  def root = "/vs"
  def filename = root + "/" + name

  @tailrec
  final def get(): Option[String] = {
    zkp.get().read(filename) match {
      case Read.OK(bytes, _) =>
        Some(new String(bytes, "UTF-8"))
      case NotFound =>
        None
      case ConnectionLost =>
        zkp.get().waitUntilConnected()
        get()
      case SessionExpired =>
        get()
    }
  }

  def set(value: =>String): Unit = {
    saveIt(value.getBytes("UTF-8"))
  }

  def clear(): Unit = {
    deleteIt()
  }

  @tailrec
  private def deleteIt(): Unit = {
    zkp.get().deleteAnyVersion(filename) match {
      case DeleteAnyVersion.OK | NotFound => // ok
      case NotEmpty =>
        // this shouldn't have happened; non-persistent nodes can't have children
      case ConnectionLost =>
        zkp.get().waitUntilConnected()
        deleteIt()
      case SessionExpired =>
        deleteIt()
    }
  }

  @tailrec
  private def saveIt(s: Array[Byte]): Unit = {
    zkp.get().writeAnyVersion(filename, s) match {
      case WriteAnyVersion.OK(_) =>
        // done
      case NotFound =>
        if(!createIt(s)) saveIt(s)
      case ConnectionLost =>
        zkp.get().waitUntilConnected()
        saveIt(s)
      case SessionExpired =>
        saveIt(s)
    }
  }

  @tailrec
  private def createIt(s: Array[Byte]): Boolean = {
    zkp.get().create(filename, s, persistent = false) match {
      case Create.OK => true
      case AlreadyExists => false
      case NoPath =>
        zkp.get().createPath(root)
        createIt(s)
      case ConnectionLost =>
        zkp.get().waitUntilConnected()
        createIt(s)
      case SessionExpired =>
        createIt(s)
    }
  }
}
