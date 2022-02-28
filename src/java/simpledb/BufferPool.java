package simpledb;

import java.io.*;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

/**
 * BufferPool manages the reading and writing of pages into memory from
 * disk. Access methods call into it to retrieve pages, and it fetches
 * pages from the appropriate location.
 * <p>
 * The BufferPool is also responsible for locking;  when a transaction fetches
 * a page, BufferPool checks that the transaction has the appropriate
 * locks to read/write the page.
 * 
 * @Threadsafe, all fields are final
 */
public class BufferPool {
    /** Bytes per page, including header. */
    private static final int DEFAULT_PAGE_SIZE = 4096;

    private static int pageSize = DEFAULT_PAGE_SIZE;
    
    /** Default number of pages passed to the constructor. This is used by
    other classes. BufferPool should use the numPages argument to the
    constructor instead. */
    public static final int DEFAULT_PAGES = 50;
    
    private int maxPages = DEFAULT_PAGES;
    private Map<PageId, Page> pageMap;
    
    // TimeStamps in Java: https://www.mkyong.com/java/how-to-get-current-timestamps-in-java/
    private HashMap<PageId, Timestamp> accTimer;

    private LockManager lockManager;

    /**
     * Creates a BufferPool that caches up to numPages pages.
     *
     * @param numPages maximum number of pages in this buffer pool.
     */
    public BufferPool(int numPages) {
        // some code goes here
        this.maxPages = numPages;
        this.pageMap = new HashMap<PageId, Page>();
        this.accTimer = new HashMap<PageId, Timestamp>();
        this.lockManager = new LockManager();
    }
    
    public static int getPageSize() {
      return pageSize;
    }
    
    // THIS FUNCTION SHOULD ONLY BE USED FOR TESTING!!
    public static void setPageSize(int pageSize) {
    	BufferPool.pageSize = pageSize;
    }
    
    // THIS FUNCTION SHOULD ONLY BE USED FOR TESTING!!
    public static void resetPageSize() {
    	BufferPool.pageSize = DEFAULT_PAGE_SIZE;
    }

    /**
     * Retrieve the specified page with the associated permissions.
     * Will acquire a lock and may block if that lock is held by another
     * transaction.
     * <p>
     * The retrieved page should be looked up in the buffer pool.  If it
     * is present, it should be returned.  If it is not present, it should
     * be added to the buffer pool and returned.  If there is insufficient
     * space in the buffer pool, a page should be evicted and the new page
     * should be added in its place.
     *
     * @param tid the ID of the transaction requesting the page
     * @param pid the ID of the requested page
     * @param perm the requested permissions on the page
     */
    public Page getPage(TransactionId tid, PageId pid, Permissions perm)
        throws TransactionAbortedException, DbException {
        // some code goes here
        //System.out.println("getPage");
        if (this.pageMap.containsKey(pid)) {
            // Try to acquire lock
            //System.out.println(perm.toString());
            this.lockManager.addLock(tid, pid, perm);
            this.accTimer.put(pid, new Timestamp(System.currentTimeMillis()));
            return this.pageMap.get(pid);
        }
        
        if (this.pageMap.size() >= this.maxPages) {
            this.evictPage();
        }
        
        this.lockManager.addLock(tid, pid, perm);
        Page resultPage = Database.getCatalog().getDatabaseFile(pid.getTableId()).readPage(pid);
        
        this.pageMap.put(pid, resultPage);
        
        // Put a timestamp
        this.accTimer.put(pid, new Timestamp(System.currentTimeMillis()));
        
        return resultPage;
    }

    /**
     * Releases the lock on a page.
     * Calling this is very risky, and may result in wrong behavior. Think hard
     * about who needs to call this and why, and why they can run the risk of
     * calling it.
     *
     * @param tid the ID of the transaction requesting the unlock
     * @param pid the ID of the page to unlock
     */
    public  void releasePage(TransactionId tid, PageId pid) {
        this.lockManager.releaseLock(tid, pid);
    }

    /**
     * Release all locks associated with a given transaction.
     *
     * @param tid the ID of the transaction requesting the unlock
     */
    public void transactionComplete(TransactionId tid) throws IOException {
        transactionComplete(tid, true);
    }

    /** Return true if the specified transaction has a lock on the specified page */
    public boolean holdsLock(TransactionId tid, PageId pid) {
        return this.lockManager.holdsLock(tid, pid);
    }

    /**
     * Commit or abort a given transaction; release all locks associated to
     * the transaction.
     *
     * When you commit, you should flush dirty pages
     * associated to the transaction to disk. When you abort, you should revert
     * any changes made by the transaction by restoring the page to its on-disk
     * state.
     *
     * Whether the transaction commits or aborts, you should also release any state the
     * BufferPool keeps regarding
     * the transaction, including releasing any locks that the transaction held.
     *
     *
     *
     * @param tid the ID of the transaction requesting the unlock
     * @param commit a flag indicating whether we should commit or abort
     */
    public synchronized void transactionComplete(TransactionId tid, boolean commit)
        throws IOException {
        // some code goes here
        // not necessary for lab1|lab2
        if (commit) {
            //System.out.println("Commit transaction");
            this.flushPages(tid);
        }
        else {
            //System.out.println("Abort transaction");
            ArrayList<PageId> discards = new ArrayList<PageId>();
            for (PageId pid : this.pageMap.keySet()) {
                //System.out.println("Page "+pid+" modified by "+this.pageMap.get(pid).isDirty());
                if(this.pageMap.get(pid).isDirty() == tid) {
                    //System.out.println("Discard abort");
                    discards.add(pid);
                }
            }
            for (PageId pid : discards) {
                discardPage(pid);
            }
        }
        this.lockManager.releaseLocks(tid);
    }

    /**
     * Add a tuple to the specified table on behalf of transaction tid.  Will
     * acquire a write lock on the page the tuple is added to and any other 
     * pages that are updated (Lock acquisition is not needed for lab2). 
     * May block if the lock(s) cannot be acquired.
     * 
     * Marks any pages that were dirtied by the operation as dirty by calling
     * their markDirty bit, and adds versions of any pages that have 
     * been dirtied to the cache (replacing any existing versions of those pages) so 
     * that future requests see up-to-date pages. 
     *
     * @param tid the transaction adding the tuple
     * @param tableId the table to add the tuple to
     * @param t the tuple to add
     */
    public void insertTuple(TransactionId tid, int tableId, Tuple t)
        throws DbException, IOException, TransactionAbortedException {
        // some code goes here
        // not necessary for lab1
        HeapFile table = (HeapFile) Database.getCatalog().getDatabaseFile(tableId);
        ArrayList<Page> modPages = table.insertTuple(tid, t);
        
        for (Page curPage : modPages) {
            if (this.pageMap.size() >= this.maxPages) {
                this.evictPage();
              }
            //System.out.println("Marking insert dirty "+curPage.getId()+" modified by "+tid);
            curPage.markDirty(true, tid);
            this.pageMap.put(curPage.getId(), curPage);
            this.accTimer.put(curPage.getId(), new Timestamp(System.currentTimeMillis()));
        }
        
    }

    /**
     * Remove the specified tuple from the buffer pool.
     * Will acquire a write lock on the page the tuple is removed from and any
     * other pages that are updated. May block if the lock(s) cannot be acquired.
     *
     * Marks any pages that were dirtied by the operation as dirty by calling
     * their markDirty bit, and adds versions of any pages that have 
     * been dirtied to the cache (replacing any existing versions of those pages) so 
     * that future requests see up-to-date pages. 
     *
     * @param tid the transaction deleting the tuple.
     * @param t the tuple to delete
     */
    public void deleteTuple(TransactionId tid, Tuple t)
        throws DbException, IOException, TransactionAbortedException {
        // some code goes here
        // not necessary for lab1
        int tableId = t.getRecordId().getPageId().getTableId();
        DbFile table = Database.getCatalog().getDatabaseFile(tableId);
        ArrayList<Page> modPages = table.deleteTuple(tid, t);
        
        for (Page curPage : modPages) {
            if (this.pageMap.size() >= this.maxPages) {
              this.evictPage();
            }
            //System.out.println("Marking delete dirty "+curPage.getId()+" modified by "+tid);
            curPage.markDirty(true, tid);
            this.pageMap.put(curPage.getId(), curPage);
            this.accTimer.put(curPage.getId(), new Timestamp(System.currentTimeMillis()));
        }
    }

    /**
     * Flush all dirty pages to disk.
     * NB: Be careful using this routine -- it writes dirty data to disk so will
     *     break simpledb if running in NO STEAL mode.
     */
    public synchronized void flushAllPages() throws IOException {
        // some code goes here
        // not necessary for lab1
        for (PageId pid : this.pageMap.keySet()) {
            this.flushPage(pid);
        }

    }

    /** Remove the specific page id from the buffer pool.
        Needed by the recovery manager to ensure that the
        buffer pool doesn't keep a rolled back page in its
        cache.
        
        Also used by B+ tree files to ensure that deleted pages
        are removed from the cache so they can be reused safely
    */
    public synchronized void discardPage(PageId pid) {
        // some code goes here
        // not necessary for lab1
        this.pageMap.remove(pid);
        this.accTimer.remove(pid);
    }

    /**
     * Flushes a certain page to disk
     * @param pid an ID indicating the page to flush
     */
    private synchronized  void flushPage(PageId pid) throws IOException {
        // some code goes here
        // not necessary for lab1
        // Flush if dirty
        HeapPage page = (HeapPage) this.pageMap.get(pid);
        if (page != null && page.isDirty() != null) {
            Database.getCatalog().getDatabaseFile(pid.getTableId()).writePage(page);
            page.markDirty(false, new TransactionId());
        }
        
    }

    /** 
     *  Write all dirty pages of the specified transaction to disk.
     */
    public synchronized  void flushPages(TransactionId tid) throws IOException {
        // some code goes here
        // not necessary for lab1|lab2
        for (PageId pid : this.pageMap.keySet()) {
            //System.out.println("commit flush?");
            //System.out.println("isDirty "+this.pageMap.get(pid).isDirty());
            if(this.pageMap.get(pid).isDirty() == tid) {
                //System.out.println("commit flush");
                this.flushPage(pid);
            }
        }
    }

    /**
     * Discards a page from the buffer pool.
     * Flushes the page to disk to ensure dirty pages are updated on disk.
     */
    private synchronized void evictPage() throws DbException {
        // some code goes here
        // not necessary for lab1
        // Evict the page that was accessed the longest time ago
        Timestamp earliest = new Timestamp(System.currentTimeMillis());
        PageId earliestPid = null;
        for (PageId curPid : this.accTimer.keySet()) {
            if (pageMap.get(curPid).isDirty() == null && this.accTimer.get(curPid).before(earliest)) {
//              System.out.println("Found earlier page");
                earliest = this.accTimer.get(curPid);
                earliestPid = curPid;
            }   
        }
        
        // Flush the earliest non-dirty page
        try {
            this.flushPage(earliestPid);
            this.discardPage(earliestPid);
//            System.out.println("Discarded the page! Only " + this.pageMap.size() + " left");
        } catch (IOException e) {
            System.out.println("Something went wrong when evicting");
        }
    }

}
