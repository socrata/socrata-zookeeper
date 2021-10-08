package com.socrata.util.locks.zookeeper

import com.socrata.zookeeper.ZooKeeperProvider
import com.socrata.util.locks.Locker
import com.socrata.util.locks.LockerTestSupport._

object ZooKeeperLockerTests {
  def main(args: Array[String]): Unit = {
    val zkp = new ZooKeeperProvider(args(0), 60000)
    val locker: Locker = new ZooKeeperLocker(zkp)
    tooManyWaitersTest(locker, locker)
    waitTooLongTest(locker, locker)
    assert(checkLockTrueIfHeldTest(locker, locker) == ((true, true, true)))
    val rwLocker = new ZooKeeperRWLocker(zkp)
    rwTooManyWaitersTest(rwLocker)
    rwWaitTooLongTest(rwLocker)
    writerPrecedenceTest(rwLocker)
    rwCheckHeldTest(rwLocker)
    rwCheckHeldSinceTest(rwLocker)
  }
}
