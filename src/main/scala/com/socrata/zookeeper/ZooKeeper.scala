package com.socrata.zookeeper

import scala.jdk.CollectionConverters._

import org.apache.{zookeeper => zk}
import zk.data.Stat
import zk.KeeperException.Code

class ZooKeeper(private var zookeeper: zk.ZooKeeper, provider: ZooKeeperProvider) {
  import ZooKeeper._

  import results._

  def reseat(): Unit = {
    zookeeper = provider.getRaw()
  }

  def waitUntilConnected(timeout: Long = Long.MaxValue) =
    provider.waitUntilConnected(zookeeper, timeout)

  type ExistsWatch = ExistsEvent => Unit
  type ReadWatch = ReadEvent => Unit
  type ChildrenWatch = ChildrenEvent => Unit

  def exists(path: String): Exists.Result = exists(path, null: zk.Watcher)
  def exists(path: String, watcher: ExistsWatch): Exists.Result = exists(path, existsWatcherize(watcher))
  def exists(path: String, watcher: zk.Watcher): Exists.Result = cl[Exists.Result](ConnectionLost, SessionExpired) {
    try {
      val scbResult = new StatCallbackResult
      zookeeper.exists(path, watcher, statCallback, scbResult)
      Exists.OK(Some(scbResult.await()))
    } catch {
      case _: zk.KeeperException.NoNodeException => Exists.OK(None)
    }
  }

  def create(path: String, persistent: Boolean = true): Create.Result = create(path, emptyByteArray, persistent)
  def create(path: String, data: Array[Byte], persistent:Boolean): Create.Result = cl[Create.Result](ConnectionLost, SessionExpired) {
    try {
      val mode = if(persistent) zk.CreateMode.PERSISTENT else zk.CreateMode.EPHEMERAL
      val scbResult = new StringCallbackResult
      zookeeper.create(path, data, zk.ZooDefs.Ids.OPEN_ACL_UNSAFE, mode, stringCallback, scbResult)
      scbResult.await()
      Create.OK
    } catch {
      case _: zk.KeeperException.NoNodeException => NoPath
      case _: zk.KeeperException.NodeExistsException => AlreadyExists
      case _: zk.KeeperException.NoChildrenForEphemeralsException => logicError("no children for ephemerals: " + path)
    }
  }

  def createWithCounter(path: String, persistent: Boolean = true): CreateWithCounter.Result = createWithCounter(path, emptyByteArray, persistent)
  def createWithCounter(path: String, data: Array[Byte], persistent:Boolean): CreateWithCounter.Result = cl[CreateWithCounter.Result](ConnectionLost, SessionExpired) {
    try {
      val mode = if(persistent) zk.CreateMode.PERSISTENT_SEQUENTIAL else zk.CreateMode.EPHEMERAL_SEQUENTIAL
      val scbResult = new StringCallbackResult
      zookeeper.create(path, data, zk.ZooDefs.Ids.OPEN_ACL_UNSAFE, mode, stringCallback, scbResult)
      CreateWithCounter.OK(scbResult.await())
    } catch {
      case _: zk.KeeperException.NoNodeException => NoPath
      case _: zk.KeeperException.NoChildrenForEphemeralsException => logicError("no children for ephemerals: " + path)
    }
  }

  def children(path: String): Children.Result = children(path, null: zk.Watcher)
  def children(path: String, watcher: ChildrenWatch): Children.Result = children(path, childrenWatcherize(watcher))
  def children(path: String, watcher: zk.Watcher): Children.Result = cl[Children.Result](ConnectionLost, SessionExpired) {
    try {
      val ccbResult = new Children2CallbackResult
      zookeeper.getChildren(path, watcher, children2Callback, ccbResult)
      val (children, stat) = ccbResult.await()
      Children.OK(children.asScala.toSet, stat)
    } catch {
      case _: zk.KeeperException.NoNodeException => NotFound
    }
  }

  def read(path: String): Read.Result = read(path, null: zk.Watcher)
  def read(path: String, watcher: ReadWatch): Read.Result = read(path, readWatcherize(watcher))
  def read(path: String, watcher: zk.Watcher): Read.Result = cl[Read.Result](ConnectionLost, SessionExpired) {
    try {
      val dcbResult = new DataCallbackResult
      zookeeper.getData(path, watcher, dataCallback, dcbResult)
      val (data, s) = dcbResult.await()
      Read.OK(data, s)
    } catch {
      case _: zk.KeeperException.NoNodeException => NotFound
    }
  }

  def write(path: String, data: Array[Byte], version: Int): Write.Result = cl[Write.Result](ConnectionLost, SessionExpired) {
    try {
      val scbResult = new StatCallbackResult
      zookeeper.setData(path, data, version, statCallback, scbResult)
      Write.OK(scbResult.await())
    } catch {
      case _: zk.KeeperException.NoNodeException => NotFound
      case _: zk.KeeperException.BadVersionException => BadVersion
    }
  }

  def writeAnyVersion(path: String, data: Array[Byte]): WriteAnyVersion.Result = cl[WriteAnyVersion.Result](ConnectionLost, SessionExpired) {
    try {
      val scbResult = new StatCallbackResult
      zookeeper.setData(path, data, -1, statCallback, scbResult)
      WriteAnyVersion.OK(scbResult.await())
    } catch {
      case _: zk.KeeperException.NoNodeException => NotFound
    }
  }

  def delete(path: String, version: Int): Delete.Result = cl[Delete.Result](ConnectionLost, SessionExpired) {
    try {
      val vcbResult = new VoidCallbackResult
      zookeeper.delete(path, version, voidCallback, vcbResult)
      vcbResult.await()
      Delete.OK
    } catch {
      case _: zk.KeeperException.NoNodeException => NotFound
      case _: zk.KeeperException.BadVersionException => BadVersion
      case _: zk.KeeperException.NotEmptyException => NotEmpty
    }
  }

  def deleteAnyVersion(path: String): DeleteAnyVersion.Result = cl[DeleteAnyVersion.Result](ConnectionLost, SessionExpired) {
    try {
      val vcbResult = new VoidCallbackResult
      zookeeper.delete(path, -1, voidCallback, vcbResult)
      vcbResult.await()
      DeleteAnyVersion.OK
    } catch {
      case _: zk.KeeperException.NoNodeException => NotFound
      case _: zk.KeeperException.NotEmptyException => NotEmpty
    }
  }

  def createPath(absolutePath: String): Unit = {
    // This either succeeds or fails, but the only way to know is to try to use the result!
    // This is a feature, not a bug, since even if it is created successfully, it might not
    // still be there when you want to use it.

    def loop(path: String): Unit = {
      create(path, persistent = true) match {
        case Create.OK =>
          /* great */
        case NoPath =>
          loop(path.substring(0, path.lastIndexOf('/')))
          loop(path)
        case ConnectionLost | SessionExpired =>
          // abandon
        case AlreadyExists =>
          // ok, it's already there..
      }
    }

    loop(absolutePath)
  }

  private def cl[T](cl: T, ex: T)(f: =>T): T =
    try {
      f
    } catch {
      case _: zk.KeeperException.ConnectionLossException =>
        cl
      case _: zk.KeeperException.SessionExpiredException =>
        ex
      case e: zk.KeeperException =>
        log.error("Unexpected exception from ZooKeeper; killing session", e)
        provider.expireIfStillCurrent(zookeeper)
        ex
    }

  private def existsWatcherize(watcher: ExistsWatch) =
    new Watcher {
      def process(event: WatchedEvent) = event match {
        case existsEvent: ExistsEvent => watcher(existsEvent)
        case _ => log.error("Received non-exists event " + event)
      }
    }

  private def readWatcherize(watcher: ReadWatch) =
    new Watcher {
      def process(event: WatchedEvent) = event match {
        case readEvent: ReadEvent => watcher(readEvent)
        case _ => log.error("Received non-read event " + event)
      }
    }

  private def childrenWatcherize(watcher: ChildrenWatch) =
    new Watcher {
      def process(event: WatchedEvent) = event match {
        case childrenEvent: ChildrenEvent => watcher(childrenEvent)
        case _ => log.error("Received non-children event " + event)
      }
    }

  private def logicError(msg: String) = throw new ZooKeeperLogicError(msg)
}

object ZooKeeper {
  val log = org.slf4j.LoggerFactory.getLogger(classOf[ZooKeeper])
  val emptyByteArray = new Array[Byte](0)

  private val sequenceIdRegex = """-(\d+)$""".r
  def sequenceId(nodeId: String) = sequenceIdRegex.findFirstMatchIn(nodeId).map(_.toString)

  object sequenceIdOrdering extends Ordering[String] {
    def compare(x: String, y: String) = sequenceId(x).getOrElse("") compareTo sequenceId(y).getOrElse("")
  }

  abstract class ResultWaiter[T] {
    private var answer: Either[zk.KeeperException, T] = _

    def result(x: T) = synchronized {
      answer = Right(x)
      notify()
    }
    def error(x: zk.KeeperException) = synchronized {
      answer = Left(x)
      notify()
    }

    def await(): T = {
      synchronized { while(answer == null) wait() }
      answer match {
        case Left(ex) => throw ex
        case Right(result) => result
      }
    }
  }

  class StatCallbackResult extends ResultWaiter[Stat]
  class StringCallbackResult extends ResultWaiter[String]
  class DataCallbackResult extends ResultWaiter[(Array[Byte], Stat)]
  class Children2CallbackResult extends ResultWaiter[(java.util.List[String], Stat)]
  class VoidCallbackResult extends ResultWaiter[Unit]

  def handleErrors(ctx: AnyRef, rc: Int, path: String): Boolean = {
    if(rc != Code.OK.intValue()) {
      ctx.asInstanceOf[ResultWaiter[_]].error(zk.KeeperException.create(Code.get(rc), path))
      true
    } else {
      false
    }
  }

  val statCallback = new zk.AsyncCallback.StatCallback {
    def processResult(rc: Int, path: String, ctx: AnyRef, stat: Stat): Unit = {
      if(!handleErrors(ctx, rc, path)) ctx.asInstanceOf[StatCallbackResult].result(stat)
    }
  }

  val stringCallback = new zk.AsyncCallback.StringCallback {
    def processResult(rc: Int, path: String, ctx: AnyRef, str: String): Unit = {
      if(!handleErrors(ctx, rc, path)) ctx.asInstanceOf[StringCallbackResult].result(str)
    }
  }

  val dataCallback = new zk.AsyncCallback.DataCallback {
    def processResult(rc: Int, path: String, ctx: AnyRef, data: Array[Byte], stat: Stat): Unit = {
      if(!handleErrors(ctx, rc, path)) ctx.asInstanceOf[DataCallbackResult].result((data, stat))
    }
  }

  val children2Callback = new zk.AsyncCallback.Children2Callback {
    def processResult(rc: Int, path: String, ctx: AnyRef, children: java.util.List[String], stat: Stat): Unit = {
      if(!handleErrors(ctx, rc, path)) ctx.asInstanceOf[Children2CallbackResult].result((children, stat))
    }
  }

  val voidCallback = new zk.AsyncCallback.VoidCallback {
    def processResult(rc: Int, path: String, ctx: AnyRef): Unit = {
      if(!handleErrors(ctx, rc, path)) ctx.asInstanceOf[VoidCallbackResult].result({})
    }
  }
}
