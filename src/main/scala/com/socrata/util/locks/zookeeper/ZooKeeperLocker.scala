package com.socrata.util.locks
package zookeeper

import scala.annotation.tailrec

import scala.{collection => sc}
import sc.{immutable => sci}

import com.socrata.util.error

import com.socrata.zookeeper
import zookeeper.ZooKeeperProvider
import zookeeper.ZooKeeper
import zookeeper.results._
import java.util.concurrent.TimeUnit
import java.nio.charset.StandardCharsets
import org.joda.time.DateTime

class ZooKeeperLocker(provider: ZooKeeperProvider) extends Locker {
  val log = org.slf4j.LoggerFactory.getLogger(classOf[ZooKeeperLocker])
  val updaterRoot = "/locks"

  def scan() {
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

  private def touch(zk: ZooKeeper, path: String) {
    @tailrec def loop() {
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

  def lock(id:String, maxWaiters: Int, timeout: Long): LockResult = {
    validateLockId(id)
    val deadline = Locker.deadlineForTimeout(timeout)

    heldLocks.get(id) match {
      case Some(unlocker) =>
        unlocker.count += 1
        Locked(unlocker)
      case None =>
        val data = populateData()

        while(true) {
          try {
            return lockIntl(provider.get(), id, data, maxWaiters, deadline)
          } catch {
            case _: Restart => {}
          }
        }
        error("Can't get here")
    }
  }

  def lockHeld(id: String): Boolean = {
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

  def lockHeldSince(id: String): Option[DateTime] = {
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

  def appendToTicket(zk: ZooKeeper, file: String, additionalData: Array[Byte]) {
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

  private def lockIntl(zk: ZooKeeper, id: String, data: Array[Byte], maxWaiters: Int, deadline: Long): LockResult = {
    val root = updaterRoot + "/" + id
    val lockBase = "lock-" + java.util.UUID.randomUUID() + "-" // important: UUIDs are all the same length
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

    @tailrec def ticketContents(ticketName: String): Option[String] = {
      zk.read(root + "/" + ticketName) match {
        case Read.OK(data, _) =>
          Some(new String(data, StandardCharsets.ISO_8859_1))
        case NotFound =>
          None
        case ConnectionLost =>
          zk.waitUntilConnected()
          ticketContents(ticketName)
        case SessionExpired =>
          restart()
      }
    }

    def firstTicket(): Option[(Long, String)] = {
      allTickets().toSeq.map { t => (t, ticket2serial(t)) }.sortBy(_._2).foreach { case (ticketName, ticketSerial) =>
        println(ticketName)
        ticketContents(ticketName).foreach { contents =>
          return Some((ticketSerial, contents))
        }
      }
      None
    }

    @tailrec def deleteTicket(ticket: String) {
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
        val tickets: List[String] = allTickets().flatMap { ticket =>
          if(ticket.startsWith(lockBase) && ticket != myTicket) { deleteTicket(ticket); Nil } // I created it, lost connection, and created a new one
          else List(ticket)
        } (sc.breakOut)

        val (precedingTicketNums, precedingTickets) = (tickets.map(ticket2serial), tickets).zipped.filter { (ticketNum, ticket) => ticketNum < mySerial }

        def gotTheLock(): Locked = {
          log.debug("Got the lock")
          // appendToTicket(zk, myTicketPath, "I own the lock\n".getBytes)
          val unlocker = new ZooUnlocker(zk, id, root, myTicket, 1)
          heldLocks += id -> unlocker
          return Locked(unlocker)
        }

        if(precedingTicketNums.isEmpty) {
          return gotTheLock()
        } else if(precedingTicketNums.size > maxWaiters) {
          deleteTicket(myTicket)
          return TooManyWaiters
        } else {
          val now = System.currentTimeMillis
          if(deadline <= now) {
            firstTicket() match {
              case Some((serial, ticketContents)) =>
                if(serial == mySerial) { // oh hey while finding the first ticket I acquired the lock!
                  return gotTheLock()
                }
                log.info("Waited too long to acquire the lock.  The holder-info is {}", ticketContents)
              case None =>
                log.warn("There are NO tickets waiting?  Not even mine?")
            }
            deleteTicket(myTicket)
            return TooLongWait
          }

          val orderedTickets: sci.SortedMap[Long, String] = precedingTicketNums.zip(precedingTickets)(sc.breakOut)
          val lastToGo = orderedTickets.last._2
          val sem = new java.util.concurrent.Semaphore(0)

          def watcher(event: zookeeper.ReadEvent) {
            import zookeeper._
            log.debug("{}",event)
            event match {
              case ConnectionStateChanged(Disconnected) | ConnectionStateChanged(Connected) =>
                /* do nothing, don't care */
              case ConnectionStateChanged(Expired) | NodeDeleted(_) | NodeDataChanged(_) =>
                sem.release()
            }
          }

          log.debug("There are " + orderedTickets.size + " users ahead of me")

          // appendToTicket(zk, myTicketPath, ("Waiting on " + lastToGo + "\nThe preceding tickets are " + orderedTickets + "\n").getBytes)

          zk.read(root + "/" + lastToGo, watcher _) match {
            case Read.OK(_, _) =>
              log.debug("Zzzzz....")
              sem.tryAcquire(deadline - now, TimeUnit.MILLISECONDS)
              log.debug("I'm awake!")
            case NotFound | ConnectionLost =>
              { /* go back around and try again */ }
            case SessionExpired =>
              restart()
          }

          // end of wait logic, now just go back to the start of the while loop and try again
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
    @tailrec private def deleteTicket() {
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

    def unlock() {
      if(count == 1) {
        heldLocks -= id
        deleteTicket()
        zk.deleteAnyVersion(root) // ignore any errors.. if necessary the cleanup-scan will get it
      } else {
        count -= 1
      }
    }
  }

  private def heldLocks = ZooKeeperLocker.heldLocks.get()
}

object ZooKeeperLocker {
  private val heldLocks = new java.lang.ThreadLocal[scala.collection.mutable.Map[String, ZooKeeperLocker#ZooUnlocker]]() {
    override def initialValue() = new scala.collection.mutable.HashMap[String, ZooKeeperLocker#ZooUnlocker]
  }
}
