package simpledb;
import java.util.HashSet;
import java.util.Random;
import java.util.concurrent.ConcurrentHashMap;

public class TransLock {
	private ConcurrentHashMap<PageId, HashSet<TransactionId>> pageSLocks;
	private ConcurrentHashMap<PageId, HashSet<TransactionId>> pageXLocks;
	private ConcurrentHashMap<TransactionId, HashSet<PageId>> transSLPage;
	private ConcurrentHashMap<TransactionId, HashSet<PageId>> transXLPage;
	private Random rand;

	TransLock() {
		pageSLocks = new ConcurrentHashMap<PageId, HashSet<TransactionId>>();
		pageXLocks = new ConcurrentHashMap<PageId, HashSet<TransactionId>>();
		transSLPage = new ConcurrentHashMap<TransactionId, HashSet<PageId>>();
		transXLPage = new ConcurrentHashMap<TransactionId, HashSet<PageId>>();
		rand = new Random();
	}
	
	boolean hasLock(TransactionId tid, PageId p) {
		if (tid == null || p == null) return false;
		addPageTrans(tid, p);
		if (transSLPage.get(tid).contains(p) && transXLPage.get(tid).contains(p)) {
			return true;
		}
		return false;
	}
	
	void addPageTrans(TransactionId tid, PageId p) {
		if (tid == null || p == null) return;
		if (!pageSLocks.containsKey(p)) pageSLocks.put(p, new HashSet<TransactionId>());
		if (!pageXLocks.containsKey(p)) pageXLocks.put(p, new HashSet<TransactionId>());
		if (!transSLPage.containsKey(tid)) transSLPage.put(tid, new HashSet<PageId>());
		if (!transXLPage.containsKey(tid)) transXLPage.put(tid, new HashSet<PageId>());
	}
	
	void sLock(TransactionId tid, PageId p) throws TransactionAbortedException {
		int interval = 3000 + rand.nextInt(2000);
		if (tid == null || p == null) return;
		addPageTrans(tid, p);
		if (transSLPage.get(tid).contains(p)) return;
		long startTime = System.currentTimeMillis();
		while (true) {
			if (pageXLocks.get(p).isEmpty() || 
				(pageXLocks.get(p).size() == 1 && pageXLocks.get(p).contains(tid))) {
				pageSLocks.get(p).add(tid);
				transSLPage.get(tid).add(p);
				return;
			} else {
				if (System.currentTimeMillis() - startTime > interval) {
					clearTransLock(tid);
					throw new TransactionAbortedException();
				}
				try {
					Thread.sleep(10);
				} catch (InterruptedException e) {
				}
			}
		}
	}
	
	void xLock(TransactionId tid, PageId p) throws TransactionAbortedException {
		if (tid == null || p == null) return;
		int interval = 3000 + rand.nextInt(2000);
		addPageTrans(tid, p);
		if (transXLPage.get(tid).contains(p)) {
			return;
		}
		long startTime = System.currentTimeMillis();
		while (true) {
			if ((pageXLocks.get(p).isEmpty() && pageSLocks.get(p).size() == 1 && pageSLocks.get(p).contains(tid))
					||(pageXLocks.get(p).isEmpty() && pageSLocks.get(p).isEmpty())) {
				pageXLocks.get(p).add(tid);
				transXLPage.get(tid).add(p);
				return;
			} else {
				if (System.currentTimeMillis() - startTime > interval) {
					clearTransLock(tid);
					throw new TransactionAbortedException();
				}
				try {
					Thread.sleep(20);
				} catch (InterruptedException e) {
				}
			}
		}
	}
	
	void sUnLock(TransactionId tid, PageId p) {
		if (tid == null || p == null) return;
		addPageTrans(tid, p);
		transSLPage.get(tid).remove(p);
		pageSLocks.get(p).remove(tid);
	}
	
	void xUnLock(TransactionId tid, PageId p) {
		if (tid == null || p == null) return;
		addPageTrans(tid, p);
		transXLPage.get(tid).remove(p);
		pageXLocks.get(p).remove(tid);
	}
	
	void clearPageLock(PageId p) {
		if (p == null) return;
		if (pageXLocks.containsKey(p)) {
			for (TransactionId tid: pageXLocks.get(p)) {
				transXLPage.get(tid).remove(p);
			}
			pageXLocks.get(p).clear();
		}
		if (pageSLocks.containsKey(p)) {
			for (TransactionId tid: pageSLocks.get(p)) {
				transSLPage.get(tid).remove(p);
			}
			pageSLocks.get(p).clear();
		}
	}
	
	void clearTransLock(TransactionId tid) {
		if (tid == null) return;
		if (transSLPage.containsKey(tid)) {
			for (PageId p: transSLPage.get(tid)) {
				pageSLocks.get(p).remove(tid);
			}
			transSLPage.get(tid).clear();
		}
		if (transXLPage.containsKey(tid)) {
			for (PageId p: transXLPage.get(tid)) {
				pageXLocks.get(p).remove(tid);
			}
			transXLPage.get(tid).clear();
		}
	}
}
