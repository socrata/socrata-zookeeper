package com.socrata.zookeeper

import org.apache.{zookeeper => zk}

import java.io.IOException
import com.socrata.util.concurrent.thread
import com.socrata.util.concurrent.Executor
import com.socrata.util.concurrent.implicits._

class ZooKeeperProvider(connectSpec: String, sessionTimeout: Int, executor: Executor) {
  def this(connectSpec: String, sessionTimeout: Int, exec: java.util.concurrent.Executor) =
    this(connectSpec, sessionTimeout, exec.asScala)

  def this(connectSpec: String, sessionTimeout: Int) =
    this(connectSpec, sessionTimeout, new Executor {
      def execute[U](r: => U) = thread() { r }
    })

  import ZooKeeperProvider._

  private var closed = false
  private var zookeeper: ZKWatcher = null

  private def openZK() = synchronized {
    if(closed) throw new IllegalStateException("ZooKeeperProvider is closed")
    try {
      zookeeper = new ZKWatcher
    } catch {
      case e: IOException => throw new ZooKeeperConnectionException(e)
    }
  }

  def expire() {
    expireIfStillCurrent(getRaw())
  }

  def close() {
    synchronized {
      if(zookeeper != null) {
        zookeeper.zookeeper.close()
        zookeeper = null
      }
      closed = true
    }
  }

  private [zookeeper] def expireIfStillCurrent(oldZK: zk.ZooKeeper) {
    waitUntilConnected(oldZK)
    val mutex = new Object
    var connected = false
    val watcher = new Watcher {
      def process(ev: WatchedEvent) = ev match {
        case ConnectionStateChanged(Connected) =>
          mutex.synchronized {
            connected = true
            mutex.notify()
          }
        case event => { log.debug("Random event: " + event) }
      }
    }
    mutex.synchronized {
      if(oldZK eq getRaw()) {
        val expirerer = new zk.ZooKeeper(connectSpec, sessionTimeout, watcher, oldZK.getSessionId(), oldZK.getSessionPasswd())
        try {
          while(!connected) {
            mutex.wait()
          }
        } finally {
          expirerer.close()
        }
      }
    }
  }

  private def nullZK(zk: ZKWatcher) = synchronized {
    executor.execute {
      zk.zookeeper.close();
    }

    if(zookeeper eq zk) {
      zookeeper = null
      notifyAll()
    }
  }

  def get() = new ZooKeeper(getRaw(), this)

  private [zookeeper] def getRaw() = synchronized {
    if(zookeeper == null) openZK()
    else if(!zookeeper.zookeeper.getState.isAlive) {
      nullZK(zookeeper)
      openZK()
    }
    zookeeper.zookeeper
  }

  private def nowPlus(timeout: Long) = {
    val end = System.currentTimeMillis() + timeout
    if(end < 0L) Long.MaxValue
    else end
  }

  // returns false iff the timeout expired.  If it returns true, the keeper may be
  // either connected or fully closed.
  private [zookeeper] def waitUntilConnected(keeper: zk.ZooKeeper, timeout: Long = Long.MaxValue): Boolean = {
    val deadline = nowPlus(timeout)

    synchronized {
      while(!closed && (keeper eq getRaw()) && (keeper.getState != zk.ZooKeeper.States.CONNECTED) && (keeper.getState != zk.ZooKeeper.States.CLOSED)) {
        log.debug("Waiting for connected-state...")

        val sleepTime = deadline - System.currentTimeMillis()
        if(sleepTime > 0L) wait(sleepTime)
        else return false;
      }

      true
    }
  }

  private def connected() = synchronized {
    notifyAll()
  }

  def pause(ms: Long) {
    // derived from the "TestableZooKeeper" object in the ZK test code
    class ProtectionByPasser(c: AnyRef) {
      def f(x: String) = {
          val field = c.getClass.getDeclaredField(x)
          field.setAccessible(true)
          field.get(c)
      }
    }
    implicit def bypassProtection(c: AnyRef) = new ProtectionByPasser(c)

    waitUntilConnected(getRaw())
    val cnxn = getRaw().f("cnxn")
    val thread = new Thread {
        setName("Zookeeper pause")
        setDaemon(true)
        override def run() {
            cnxn.synchronized {
                import java.nio.channels._
                cnxn.f("sendThread").f("sockKey").asInstanceOf[SelectionKey].channel.asInstanceOf[SocketChannel].socket.close()
                Thread.sleep(ms)
            }
        }
    }
    thread.start()
  }

  private class ZKWatcher extends Watcher {
    val zookeeper = new zk.ZooKeeper(connectSpec, sessionTimeout, this)

    def process(event: WatchedEvent) = {
      log.debug("{}",event)
      event match {
        case ConnectionStateChanged(Expired) =>
          nullZK(this)
          log.error("SESSION_EXPIRED from zookeeper.  This should be rare!  Ideally, it should not happen at all.")
        case ConnectionStateChanged(Connected) =>
          connected()
        case ConnectionStateChanged(Disconnected) =>
          { /* nothing */ }
        case NodeEvent(_) =>
          log.warn("Connection provider got a notification for data change; someone just passed \"true\" as a watch parameter");
      }
    }
  }

  openZK()
}

object ZooKeeperProvider {
  val log = org.slf4j.LoggerFactory.getLogger(classOf[ZooKeeperProvider])
}
