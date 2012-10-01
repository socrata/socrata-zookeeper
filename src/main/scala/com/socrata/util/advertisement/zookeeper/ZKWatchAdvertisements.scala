package com.socrata.util.advertisement
package zookeeper

import java.net.URLDecoder

import com.socrata.zookeeper._
import com.socrata.zookeeper.results._

import com.socrata.util.concurrent.Executor
import com.socrata.util.logging.LazyStringLogger

import ZKWatchAdvertisements._

class ZKWatchAdvertisements(zkp: ZooKeeperProvider, private val executor: Executor, root: String) extends WatchAdvertisements {
  private var onChange: WatchAdvertisements => Unit = null
  private val watcher = new AdvertWatcher(this)
  @volatile private var nodes = Set.empty[String]
  private var state = Unstarted
  @volatile private var unhandledChangeEventInFlight = false

  final def start(onChange: WatchAdvertisements => Unit) {
    synchronized {
      if(state == Stopped) throw new IllegalStateException("Cannot be restarted")
      if(state == Started) throw new IllegalStateException("Already started")
      state = Started
      this.onChange = onChange
    }
    reloadNodes()
  }

  def current = nodes

  def stop() {
    synchronized {
      state = Stopped

      // minimize leaks; the watcher will still exist until triggerd,
      // but it won't have a reference to us.
      watcher.clear()

      if(nodes.nonEmpty) {
        nodes = Set.empty[String]
        executor.execute {
          onChange(this)
        }
      }
    }
  }

  private def reloadNodes() {
    while(true) {
      val waitBeforeRetry = synchronized {
        if(state == Stopped) {
          log.trace("We've been stopped; not reloading the nodes")
          return
        }
        val zk = try {
          zkp.get()
        } catch {
          case _: IllegalStateException =>
            log.trace("ZK shut down; stopping the advert watcher instead of reloading nodes")
            stop()
            return
        }
        zk.createPath(root)
        zk.children(root, watcher) match {
          case Children.OK(kids, _) =>
            log.trace("Found " + kids.size + " children")
            val newNodes = kids.map(URLDecoder.decode(_, "UTF-8"))
            if(nodes != newNodes) {
              nodes = newNodes
              if(!unhandledChangeEventInFlight) {
                unhandledChangeEventInFlight = true
                executor.execute {
                  unhandledChangeEventInFlight = false
                  onChange(this)
                }
              }
            }
            return
          case NotFound =>
            false
          case _ =>
            true
        }
      }
      if(waitBeforeRetry) {
        val zk = try {
          zkp.get()
        } catch {
          case _: IllegalStateException =>
            log.trace("ZK shut down; stopping the advert watcher instead of waiting for reconnect")
            stop()
            return
        }
        zk.waitUntilConnected()
      }
    }
  }
}

object ZKWatchAdvertisements {
  private val log = LazyStringLogger[WatchAdvertisements]

  private final val Unstarted = 0
  private final val Started = 1
  private final val Stopped = 2

  private class AdvertWatcher(private var advertisement: ZKWatchAdvertisements) extends Watcher {
    def clear() {
      synchronized {
        advertisement = null
      }
    }

    def process(ev: WatchedEvent) {
      ev match {
        case ConnectionStateChanged(Disconnected) =>
          log.trace("Got disconnected")
        case _ =>
          log.trace("got event; kicking reregister")
          synchronized {
            if(advertisement != null) advertisement.executor.execute { advertisement.reloadNodes() }
          }
      }
    }
  }
}
