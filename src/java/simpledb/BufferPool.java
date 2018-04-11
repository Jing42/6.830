package simpledb;

import java.io.*;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Random;
import java.util.concurrent.ConcurrentHashMap;

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
    private int numPages;
    private Page[] pages;
    private Random rand;
    ConcurrentHashMap<TransactionId, HashSet<PageId>> transPages;
    TransLock tLock;

    /**
     * Creates a BufferPool that caches up to numPages pages.
     *
     * @param numPages maximum number of pages in this buffer pool.
     */
    public BufferPool(int numPages) {
        // some code goes here
    	this.numPages = numPages;
    	pages = new Page[numPages];
    	rand = new Random();
    	tLock = new TransLock();
    	transPages = new ConcurrentHashMap<TransactionId, HashSet<PageId>>();
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
    public  Page getPage(TransactionId tid, PageId pid, Permissions perm)
            throws TransactionAbortedException, DbException {
            // some code goes here
	    	if (perm == Permissions.READ_ONLY) {
	    		tLock.sLock(tid, pid);
	    	} else if(perm == Permissions.READ_WRITE) {
	    		tLock.xLock(tid, pid);
	    	}
        	int vacancy = -1;
        	while (vacancy == -1) {
    	    	for (int i=0; i < pages.length; i++) {
    	    		if (pages[i] == null) {
    	    			vacancy = i;
    	    			break;
    	    		}
    	    		if (pages[i].getId().equals(pid)) {
    	    			return pages[i];
    	    		}
    	    	}
    	    	if (vacancy == -1) {
    	    		evictPage();
    	    	}
        	}
        	DbFile dbfile = Database.getCatalog().getDatabaseFile(pid.getTableId());
        	Page page = dbfile.readPage(pid);
        	pages[vacancy] = page;
        	return page;
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
        // some code goes here
        // not necessary for lab1|lab2
    	tLock.sUnLock(tid, pid);
    	tLock.xUnLock(tid, pid);
    }

    /**
     * Release all locks associated with a given transaction.
     *
     * @param tid the ID of the transaction requesting the unlock
     */
    public void transactionComplete(TransactionId tid) throws IOException {
        // some code goes here
        // not necessary for lab1|lab2
    	transactionComplete(tid, true);
    }

    /** Return true if the specified transaction has a lock on the specified page */
    public boolean holdsLock(TransactionId tid, PageId p) {
        // some code goes here
        // not necessary for lab1|lab2
    	return tLock.hasLock(tid, p);
    }
    /**
     * Commit or abort a given transaction; release all locks associated to
     * the transaction.
     *
     * @param tid the ID of the transaction requesting the unlock
     * @param commit a flag indicating whether we should commit or abort
     */
    public void transactionComplete(TransactionId tid, boolean commit)
        throws IOException {
        // some code goes here
        // not necessary for lab1|lab2
    	if (commit) {
    		flushPages(tid);
    	} else {
    		if (transPages.containsKey(tid)) {
	    		for (PageId pid: transPages.get(tid)) {
	    			DbFile dbfile = Database.getCatalog().getDatabaseFile(pid.getTableId());
	    			Page page = dbfile.readPage(pid);
	    			for (int i = 0; i < pages.length; i++) {
	    				if (pages[i].getId() == pid) {
	    					pages[i] = page;
	    					break;
	    				}    				
	    			}
	    		}
    		}
    	}
    	if (transPages.containsKey(tid)) {
    		transPages.get(tid).clear();
    	}
    	tLock.clearTransLock(tid);
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
    	DbFile dbfile = Database.getCatalog().getDatabaseFile(tableId);
    	ArrayList<Page> res = dbfile.insertTuple(tid, t);
    	for (Page i: res) {
    		i.markDirty(true, tid);
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
    public  void deleteTuple(TransactionId tid, Tuple t)
        throws DbException, IOException, TransactionAbortedException {
        // some code goes here
        // not necessary for lab1
    	DbFile dbfile = Database.getCatalog().getDatabaseFile(t.getRecordId().getPageId().getTableId());
    	ArrayList<Page> res = dbfile.deleteTuple(tid, t);
    	for (Page i: res) {
    		i.markDirty(true, tid);
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
    	for (Page i: pages) {
    		if (i == null) continue;
    		flushPage(i.getId());
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
    	for (int i = 0; i < numPages; i++) {
    		if (pages[i].getId() == pid) {
    			pages[i] = null;
    			return;
    		}
    	}
    }

    /**
     * Flushes a certain page to disk
     * @param pid an ID indicating the page to flush
     */
    private synchronized  void flushPage(PageId pid) throws IOException {
        // some code goes here
        // not necessary for lab1
    	DbFile f = Database.getCatalog().getDatabaseFile(pid.getTableId());
    	for (Page p: pages) {
    		if (p != null && p.getId() == pid) {
    			f.writePage(p);
    		}
    	}
    	f.readPage(pid).markDirty(false, null);
    	tLock.clearPageLock(pid);
    }

    /** Write all pages of the specified transaction to disk.
     */
    public synchronized  void flushPages(TransactionId tid) throws IOException {
        // some code goes here
        // not necessary for lab1|lab2
    	if (transPages.containsKey(tid)) {
    		for (PageId pid: transPages.get(tid)) {
    			flushPage(pid);
    		}
    	}
    }

    /**
     * Discards a page from the buffer pool.
     * Flushes the page to disk to ensure dirty pages are updated on disk.
     */
    private synchronized  void evictPage() throws DbException {
        // some code goes here
        // not necessary for lab1
    	int count = 0;
    	int n = rand.nextInt(numPages);
    	while (true) {
    		if (count > pages.length) {
    			throw new DbException("All pages are dirty, can't evict!");
    		}
    		count ++;
    		n++;
    		n %= pages.length;
    		if (pages[n] == null) return; 
    		if ((pages[n].isDirty() != null)) continue;
    		try {
				flushPage(pages[n].getId());
			} catch (Exception e) {
				continue;
			}
    		discardPage(pages[n].getId());
    		return;
    	}
    }

}
