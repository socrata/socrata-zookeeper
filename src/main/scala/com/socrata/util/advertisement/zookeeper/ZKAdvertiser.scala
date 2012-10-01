package com.socrata.util.advertisement
package zookeeper

import scala.annotation.tailrec

import java.net.URLEncoder

import com.socrata.zookeeper._
import com.socrata.zookeeper.results._

import com.socrata.util.concurrent.Executor
import com.socrata.util.logging.LazyStringLogger

import ZKAdvertiser._

class ZKAdvertiser(zkp: ZooKeeperProvider, executor: Executor, root: String) extends Advertiser {
  def advertise(filename: String): AdvertisementRegistration = {
    val reg = new RegistrationImpl(root, filename)
    createNode(reg)
    reg
  }

  @tailrec
  private def deregister(registration: RegistrationImpl) {
    val zk = try {
      zkp.get()
    } catch {
      case _: IllegalStateException =>
        log.trace("ZKP is closed; cannot delete node " + registration.filename + " but it will go away on its own soon anyway")
        return
    }

    registration.synchronized {
      registration.reregister = false
      log.trace("deleting " + registration.pathname)
      zk.deleteAnyVersion(registration.pathname)
    } match {
      case DeleteAnyVersion.OK | NotFound =>
        log.trace("done")
      case NotEmpty =>
        log.trace(registration.pathname + " had child nodes?")
      case ConnectionLost | SessionExpired =>
        log.trace("connection lost")
        zk.waitUntilConnected()
        log.trace("trying again")
        deregister(registration)
    }
  }

  @tailrec
  private def createNode(registration: RegistrationImpl) {
    val zk = try {
      zkp.get()
    } catch {
      case _: IllegalStateException =>
        log.trace("ZKP is closed; not creating node " + registration.filename)
        return
    }

    registration.synchronized {
      if(!registration.reregister) {
        log.trace("We've been de-registered; not re-creating the node")
        return
      }
      zk.createPath(registration.path)
      log.trace("creating " + registration.pathname)
      zk.create(registration.pathname, persistent = false)
    } match {
      case Create.OK | AlreadyExists =>
        log.trace("setting up watch on " + registration.pathname)
        zk.read(registration.pathname, registration) match {
          case Read.OK(_, _) =>
            log.trace("ok")
          case _ =>
            log.trace("oops; restarting create process")
            createNode(registration)
        }
      case NoPath =>
        log.trace("no path; trying again")
        createNode(registration)
      case ConnectionLost | SessionExpired =>
        log.trace("connection lost")
        zk.waitUntilConnected()
        log.trace("trying again")
        createNode(registration)
    }
  }

  private class RegistrationImpl(val path: String, val filename: String) extends Watcher with AdvertisementRegistration {
    var reregister: Boolean = true

    def pathname = path + "/" + URLEncoder.encode(filename, "UTF-8")

    def process(ev: WatchedEvent) = ev match {
      case ConnectionStateChanged(Disconnected) =>
        log.trace("Got disconnected")
      case _ =>
        log.trace("got event" + ev + "; kicking reregister")
        executor.execute { createNode(this) }
    }

    def stopAdvertising() {
      deregister(this)
    }
  }
}

object ZKAdvertiser {
  private val log = LazyStringLogger[ZKAdvertiser]
}
