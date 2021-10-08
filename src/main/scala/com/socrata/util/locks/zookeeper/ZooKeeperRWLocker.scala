package com.socrata.util.locks
package zookeeper

import scala.annotation.tailrec

import scala.{collection => sc}

import com.socrata.util.error

import com.socrata.zookeeper
import zookeeper.ZooKeeperProvider
import zookeeper.ZooKeeper
import zookeeper.results._
import java.util.concurrent.TimeUnit
import org.joda.time.DateTime

class ZooKeeperRWLocker(provider: ZooKeeperProvider) extends RWLocker {
  val log = org.slf4j.LoggerFactory.getLogger(classOf[ZooKeeperLocker])
  val updaterRoot = "/rwlocks"
  
  def scan(): Unit = {
    val zk = provider.get()
    zk.children(updaterRoot) match {
      case Children.OK(children, _) =>
        for(child <- children) zk.deleteAnyVersion(updaterRoot + "/" + child)
      case _ =>
        {}
    }
  }
  
  private class Restart extends scala.util.control.ControlThrowable
  private def restart() = throw new Restart
  
  private def touch(zk: ZooKeeper, path: String): Unit = {
    @tailrec def loop(): Unit = {
      zk.create(path, persistent = true) match {
        case Create.OK | AlreadyExists =>
          log.debug("Touched " + path)
        case NoPath =>
          touch(zk, path.substring(0, path.lastIndexOf('/')))
          touch(zk, path)
        case ConnectionLost =>
          zk.waitUntilConnected()
          loop()
        case SessionExpired =>
          restart()
      }
    }
    loop()
  }
  
  def populateData(): Array[Byte] = ZooKeeper.emptyByteArray
  
  def readLock(id:String, maxWaiters: Int, timeout: Long): LockResult = {
    if(id.isEmpty) throw new IllegalArgumentException("A lock-name must not be empty")
    if(id.contains('/')) throw new IllegalArgumentException("A lock ID cannot contain a slash")
    val deadline = Locker.deadlineForTimeout(timeout)

    if(heldWriteLocks.contains(id)) throw new IllegalStateException("Attempt to take a read-lock while holding the write half")
    heldReadLocks.get(id) match {
      case Some(unlocker) =>
        unlocker.count += 1
        Locked(unlocker)
      case None =>
        val data = populateData()
    
        while(true) {
          try {
            return readLockIntl(provider.get(), id, data, maxWaiters, deadline)
          } catch {
            case _: Restart => {}
          }
        }
        error("Can't get here")
    }
  }

  def writeLock(id:String, maxWaiters: Int, timeout: Long): LockResult = {
    if(id.isEmpty) throw new IllegalArgumentException("A lock-name must not be empty")
    if(id.contains('/')) throw new IllegalArgumentException("A lock ID cannot contain a slash")
    val deadline = Locker.deadlineForTimeout(timeout)

    if(heldReadLocks.contains(id)) throw new IllegalStateException("Attempt to take a write-lock while holding the read half")
    heldWriteLocks.get(id) match {
      case Some(unlocker) =>
        unlocker.count += 1
        Locked(unlocker)
      case None =>
        val data = populateData()

        while(true) {
          try {
            return writeLockIntl(provider.get(), id, data, maxWaiters, deadline)
          } catch {
            case _: Restart => {}
          }
        }
        error("Can't get here")
    }
  }

  def checkReadOrWriteLock(id: String): Boolean = {
    validateLockId(id)

    def loop(): Boolean = {
      val zk = provider.get()
      zk.children(updaterRoot + "/" + id) match {
        case Children.OK(ids, _) =>
          ids.nonEmpty
        case NotFound =>
          false
        case ConnectionLost | SessionExpired =>
          zk.waitUntilConnected()
          loop()
      }
    }
    loop()
  }

  def checkWriteLock(id: String): Boolean = {
    validateLockId(id)

    def loop(): Boolean = {
      val zk = provider.get()
      zk.children(updaterRoot + "/" + id) match {
        case Children.OK(ids, _) =>
          ids.exists(_.startsWith("w"))
        case NotFound =>
          false
        case ConnectionLost | SessionExpired =>
          zk.waitUntilConnected()
          loop()
      }
    }
    loop()
  }

  def checkReadOrWriteLockTime(id: String): Option[DateTime] = {
    validateLockId(id)

    def nodeIdLoop(): Option[String] = {
      val zk = provider.get()
      zk.children(updaterRoot + "/" + id) match {
        case Children.OK(ids, _) =>
          if(ids.isEmpty) None else Some(ids.min(ZooKeeper.sequenceIdOrdering))
        case NotFound =>
          None
        case ConnectionLost | SessionExpired =>
          zk.waitUntilConnected()
          nodeIdLoop()
      }
    }
    val nodeId = nodeIdLoop()

    def cTimeLoop(): Option[DateTime] = {
      val zk = provider.get()
      nodeId.flatMap { nid =>
        zk.read(updaterRoot + "/" + id + "/" + nid) match {
          case Read.OK(_, stat) =>
            Some(new DateTime(stat.getCtime))
          case NotFound =>
            None
          case ConnectionLost | SessionExpired =>
            zk.waitUntilConnected()
            cTimeLoop()
        }
      }
    }
    cTimeLoop()
  }

  def checkWriteLockTime(id: String): Option[DateTime] = {
    validateLockId(id)

    def nodeIdLoop(): Option[String] = {
      val zk = provider.get()
      zk.children(updaterRoot + "/" + id) match {
        case Children.OK(ids, _) =>
          val writeIds = ids.filter(_.startsWith("w"))
          if(writeIds.isEmpty) None else Some(writeIds.min(ZooKeeper.sequenceIdOrdering))
        case NotFound =>
          None
        case ConnectionLost | SessionExpired =>
          zk.waitUntilConnected()
          nodeIdLoop()
      }
    }
    val nodeId = nodeIdLoop()

    def cTimeLoop(): Option[DateTime] = {
      val zk = provider.get()
      nodeId.flatMap { nid =>
        zk.read(updaterRoot + "/" + id + "/" + nid) match {
          case Read.OK(_, stat) =>
            Some(new DateTime(stat.getCtime))
          case NotFound =>
            None
          case ConnectionLost | SessionExpired =>
            zk.waitUntilConnected()
            cTimeLoop()
        }
      }
    }
    cTimeLoop()
  }

  def appendToTicket(zk: ZooKeeper, file: String, additionalData: Array[Byte]): Unit = {
      def read(): (Array[Byte], Int) =
          zk.read(file) match {
              case Read.OK(data, stat) =>
                  (data, stat.getVersion)
              case NotFound =>
                  (Array[Byte](), -1)
              case ConnectionLost =>
                  zk.waitUntilConnected()
                  read()
              case SessionExpired =>
                  restart()
          }
      
      def write(data: Array[Byte], version: Int): Unit =
          zk.write(file, data, version) match {
              case Write.OK(_) | BadVersion | NotFound => {}
              case ConnectionLost =>
                  zk.waitUntilConnected()
                  write(data, version)
              case SessionExpired =>
                  restart()
          }
      
      val (oldData, version) = read()
      write(oldData ++ additionalData, version)
  }

  private sealed abstract class LockState
  private case object TooLocked extends LockState
  private case class GotIt(held: scala.collection.mutable.Map[String, ZooUnlocker]) extends LockState
  private case class Wait(ticket: String) extends LockState

  private def isWriteTicket(ticket: String) = ticket.startsWith("w-lock-")

  private def readLockIntl(zk: ZooKeeper, id: String, data: Array[Byte], maxWaiters:Int, deadline: Long) = {
    def findTicketToAwait(myTicket: String, allTickets: List[String]) = {
      val writersAheadOfMe = allTickets.takeWhile(_ != myTicket).filter(isWriteTicket)
      if(writersAheadOfMe.isEmpty) GotIt(heldReadLocks)
      else if(writersAheadOfMe.length >= maxWaiters) TooLocked
      else Wait(writersAheadOfMe.last)
    }
    
    lockIntl(zk, id, data, "r", findTicketToAwait, deadline)
  }

  private def writeLockIntl(zk: ZooKeeper, id: String, data: Array[Byte], maxWaiters: Int, deadline: Long) = {
    def findTicketToAwait(myTicket: String, allTickets: List[String]) = {
      val ticketsAheadOfMe = allTickets.takeWhile(_ != myTicket)
      if(ticketsAheadOfMe.isEmpty) GotIt(heldWriteLocks)
      else if(ticketsAheadOfMe.filter(isWriteTicket).length >= maxWaiters) TooLocked
      else Wait(ticketsAheadOfMe.last)
    }

    lockIntl(zk, id, data, "w", findTicketToAwait, deadline)
  }

  private def lockIntl(zk: ZooKeeper, id: String, data: Array[Byte], lockBaseBase: String, ahead: (String, List[String]) => LockState, deadline: Long): LockResult = {
    val root = updaterRoot + "/" + id
    val lockBase = lockBaseBase + "-lock-" + java.util.UUID.randomUUID() + "-" // important: UUIDs are all the same length
    val ticketBase = root + "/" + lockBase
    
    def ticket2serial(ticketName: String) = ticketName.substring(lockBase.length).toLong
    
    @tailrec def getTicket(): String = {
      touch(zk, root)
      zk.createWithCounter(ticketBase, data, persistent = false) match {
        case CreateWithCounter.OK(path) =>
          path.substring(root.length + 1)
        case NoPath =>
          getTicket()
        case ConnectionLost =>
          // The case of "create but lose connection before response gets here"
          // is handled below, in the flatMap on allTickets()
          zk.waitUntilConnected()
          getTicket()
        case SessionExpired =>
          restart()
      }
    }

    @tailrec def allTickets(): Set[String] = {
      zk.children(root) match {
        case Children.OK(children, _) =>
          children
        case NotFound =>
          log.error("Somehow, my ticket has gone away along with the ticket-directory")
          restart()
        case ConnectionLost =>
          zk.waitUntilConnected()
          allTickets()
        case SessionExpired =>
          restart()
      }
    }

    @tailrec def deleteTicket(ticket: String): Unit = {
      zk.deleteAnyVersion(root + "/" + ticket) match {
        case DeleteAnyVersion.OK | NotFound => { /* ok */ }
        case NotEmpty =>
          throw new LockInvariantFailedException("There is a child of ticket " + root + "/" + ticket + "!!!!!")
        case ConnectionLost =>
          zk.waitUntilConnected()
          deleteTicket(ticket)
        case SessionExpired =>
          restart()
      }
    }

    val myTicket = getTicket()
    try {
      val myTicketPath = root + "/" + myTicket
      log.debug("My lock ticket is " + myTicket)
      val mySerial = ticket2serial(myTicket)
      
      while(true) {
        val tickets: List[String] = allTickets().iterator.flatMap { ticket =>
          if(ticket.startsWith(lockBase) && ticket != myTicket) { deleteTicket(ticket); Iterator.empty } // I created it, lost connection, and created a new one
          else Iterator(ticket)
        }.toList

        ahead(myTicket, tickets.sortBy(ticket2serial)) match {
          case GotIt(heldLocks) =>
            log.debug("Got the lock")
            // appendToTicket(zk, myTicketPath, "I own the lock\n".getBytes)
            val unlocker = new ZooUnlocker(zk, id, root, myTicket, 1)
            heldLocks += id -> unlocker
            return Locked(unlocker)
          case TooLocked =>
            deleteTicket(myTicket)
            return TooManyWaiters
          case Wait(thisTicket) =>
            val now = System.currentTimeMillis
            if(deadline <= now) {
              deleteTicket(myTicket)
              return TooLongWait
            }

            val sem = new java.util.concurrent.Semaphore(0)

            def watcher(event: zookeeper.ReadEvent): Unit = {
              import zookeeper._
              log.debug("{}",event)

              event match {
                case ConnectionStateChanged(Disconnected) | ConnectionStateChanged(Connected) =>
                  /* do nothing, don't care */
                case ConnectionStateChanged(Expired) | NodeDeleted(_) | NodeDataChanged(_) =>
                  sem.release()
              }
            }

            zk.read(root + "/" + thisTicket, watcher _) match {
              case Read.OK(_, _) =>
                log.debug("Zzzzz....")
                sem.tryAcquire(deadline - now, TimeUnit.MILLISECONDS)
                log.debug("I'm awake!")
              case NotFound | ConnectionLost =>
                { /* go back around and try again */ }
              case SessionExpired =>
                restart()
            }
        }
      }
      error("Can't get here")
    } catch {
      case e: Exception =>
        log.warn("Exiting abnormally; deleting ticket", e)
        deleteTicket(myTicket)
        throw e
    }
  }
  
  private class ZooUnlocker(zk: ZooKeeper, id: String, root: String, myTicket: String, var count: Int) extends Unlocker {
    @tailrec private def deleteTicket(): Unit = {
      zk.deleteAnyVersion(root + "/" + myTicket) match {
        case DeleteAnyVersion.OK =>
          log.debug("Lock release successful")
        case NotFound =>
          log.warn("Didn't find my ticket to delete???")
        case ConnectionLost =>
          zk.waitUntilConnected()
          deleteTicket()
        case NotEmpty =>
          throw new LockInvariantFailedException("There is a child of ticket " + root + "/" + myTicket + "????")
        case SessionExpired =>
          log.warn("Session expired; my ticket is no longer there to remove")
      }
    }
    
    def unlock(): Unit = {
      if(count == 1) {
        heldReadLocks -= id // I can only be holding one at a time...
        heldWriteLocks -= id // ...so remove from both
        deleteTicket()
        zk.deleteAnyVersion(root) // ignore any errors.. if necessary the cleanup-scan will get it
      } else {
        count -= 1
      }
    }
  }
  
  private def heldReadLocks = heldReadLocksTL.get()
  private def heldWriteLocks = heldWriteLocksTL.get()

  private val heldReadLocksTL = new java.lang.ThreadLocal[scala.collection.mutable.Map[String, ZooUnlocker]]() {
    override def initialValue() = new scala.collection.mutable.HashMap[String, ZooUnlocker]
  }
  private val heldWriteLocksTL = new java.lang.ThreadLocal[scala.collection.mutable.Map[String, ZooUnlocker]]() {
    override def initialValue() = new scala.collection.mutable.HashMap[String, ZooUnlocker]
  }
}
