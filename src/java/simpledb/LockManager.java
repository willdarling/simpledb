package simpledb;

import java.util.concurrent.ConcurrentHashMap;
import java.util.ArrayList;

/**
 * A (hopefully) simple lock manager used by BufferPool to manage its
 * page locks.
 */

public class LockManager {

  private ConcurrentHashMap<PageId, TransactionId> exclusiveLocks;
  private ConcurrentHashMap<PageId, ArrayList<TransactionId>> sharedLocks;
  private ConcurrentHashMap<TransactionId, Integer> accessCount;
  private ConcurrentHashMap<TransactionId, ArrayList<PageId>> transactionLocks;

  private int TIMEOUT = 10;
  private int LIMIT = 20;

  public LockManager() {
    this.exclusiveLocks = new ConcurrentHashMap<PageId, TransactionId>();
    this.sharedLocks = new ConcurrentHashMap<PageId, ArrayList<TransactionId>>();
    this.accessCount = new ConcurrentHashMap<TransactionId, Integer>();
    this.transactionLocks = new ConcurrentHashMap<TransactionId, ArrayList<PageId>>();
  }

  /**
   * Attempt to acquire a lock for the given page based on the given permissions.
   * Blocks if the lock cannot be acquired.
   *
   * @param tid the ID of the transaction requesting the page lock
   * @param pid the ID of the requested page
   * @param perm the requested permissions on the page
   */
  public synchronized void addLock(TransactionId tid, PageId pid, Permissions perm) 
    throws TransactionAbortedException {
    //Each transaction keeps track of the locks it holds or is trying to get a hold of
    
    ArrayList<PageId> locks = transactionLocks.get(tid);
      if (locks == null) {
        locks = new ArrayList<PageId>();
      }
    locks.add(pid);
    transactionLocks.put(tid, locks);
    
    //Add a shared lock
    if (perm == Permissions.READ_ONLY) {
      //System.out.println("Try to get read");
      //If you already have this read lock return
      if (sharedLocks.containsKey(pid) && sharedLocks.get(pid).contains(tid)) {
        //System.out.println("Already have read");
        return;
      }
      //If you have the only write lock, get a read in return 
      if (exclusiveLocks.remove(pid, tid)) {
        ArrayList<TransactionId> owners = new ArrayList<TransactionId>();
        owners.add(tid);
        sharedLocks.put(pid, owners);
        return;
      }
      //Loop if there's currently an exclusive lock being held, stop looping after trying to
      //access the lock after LIMIT times
      while (exclusiveLocks.containsKey(pid)) {
        incrementCounter(tid);
        try {
          Thread.sleep(TIMEOUT);
        } catch(InterruptedException e) {
          e.printStackTrace();
        }
      }
      resetCounter(tid);
      //System.out.println("Got read");
      ArrayList<TransactionId> owners = sharedLocks.get(pid);
      if (owners == null) {
        owners = new ArrayList<TransactionId>();
      }
      owners.add(tid);
      sharedLocks.put(pid, owners);
    }

    //Add an exclusive lock
    else {
      //System.out.println("Try to get write");
      //Return if you've already got the exclusive lock
      if (exclusiveLocks.get(pid) == tid) {
        //System.out.println("Already have write");
        return;
      }
      //Upgrade the lock if you're the only read lock on this page
      if (sharedLocks.containsKey(pid) && sharedLocks.get(pid).contains(tid) && sharedLocks.get(pid).size() == 1) {
        //System.out.println("Upgrade");
        sharedLocks.remove(pid);
        exclusiveLocks.put(pid, tid);
        return;
      }
      //Loop if there's currently any exclusive or shared locks held on this page
      while (exclusiveLocks.containsKey(pid) || sharedLocks.containsKey(pid)) {
        //System.out.println(exclusiveLocks.containsKey(pid));
        //System.out.println(sharedLocks.containsKey(pid));
        incrementCounter(tid);
        try {
          Thread.sleep(TIMEOUT);
        } catch(InterruptedException e) {
          e.printStackTrace();
        }
      }
      resetCounter(tid);
      //System.out.println("Got write");
      exclusiveLocks.put(pid, tid);
    }
  }

  public synchronized void releaseLock(TransactionId tid, PageId pid) {
    //Check exclusive locks
    exclusiveLocks.remove(pid, tid);
      //System.out.println("Release write lock");

    //Check shared locks
    ArrayList<TransactionId> owners = sharedLocks.remove(pid);
    if (owners != null && owners.size() > 1) {
      //System.out.println("Release one lock on pid");
      owners.remove(tid);
      sharedLocks.put(pid, owners);
    }
    //else {
      //System.out.println("Release all lock on pid");
    //}
  }

  public synchronized boolean holdsLock(TransactionId tid, PageId pid) {
    //Check exclusive locks
    if (this.exclusiveLocks.containsKey(pid)) {
      return tid == this.exclusiveLocks.get(pid);
    }
    //Check shared locks
    else if (this.sharedLocks.containsKey(pid)) {
      return this.sharedLocks.get(pid).contains(tid);
    }
    return false;
  }

  public synchronized void releaseLocks(TransactionId tid) {
    //System.out.println("releasing locks");
    ArrayList<PageId> locks = transactionLocks.remove(tid);
    if (locks == null) {
      return;
    }
    for (PageId pid : locks) {
      releaseLock(tid, pid);
    }
  }

  private synchronized void incrementCounter(TransactionId tid) 
    throws TransactionAbortedException {

    Integer count = accessCount.get(tid);
    if (count == null) {
      count = new Integer(0);
    }
    count = new Integer(count.intValue()+1);
    //System.out.println(String.format("Count = %d", count.intValue()));
    if (count.intValue() > LIMIT) {
      throw new TransactionAbortedException();
    }
    else {
      accessCount.put(tid, count);
    }
  }

  private synchronized void resetCounter(TransactionId tid) {

    Integer count = new Integer(0);
    accessCount.put(tid, count);
  }
}










