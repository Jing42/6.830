package simpledb;

import java.io.*;
import java.util.*;

/**
 * HeapFile is an implementation of a DbFile that stores a collection of tuples
 * in no particular order. Tuples are stored on pages, each of which is a fixed
 * size, and the file is simply a collection of those pages. HeapFile works
 * closely with HeapPage. The format of HeapPages is described in the HeapPage
 * constructor.
 * 
 * @see simpledb.HeapPage#HeapPage
 * @author Sam Madden
 */
public class HeapFile implements DbFile {

	private File f;
	private TupleDesc td;
	
    /**
     * Constructs a heap file backed by the specified file.
     * 
     * @param f
     *            the file that stores the on-disk backing store for this heap
     *            file.
     */
    public HeapFile(File f, TupleDesc td) {
        // some code goes here
    	this.f = f;
    	this.td = td;
    }

    /**
     * Returns the File backing this HeapFile on disk.
     * 
     * @return the File backing this HeapFile on disk.
     */
    public File getFile() {
        // some code goes here
        return f;
    }

    /**
     * Returns an ID uniquely identifying this HeapFile. Implementation note:
     * you will need to generate this tableid somewhere to ensure that each
     * HeapFile has a "unique id," and that you always return the same value for
     * a particular HeapFile. We suggest hashing the absolute file name of the
     * file underlying the heapfile, i.e. f.getAbsoluteFile().hashCode().
     * 
     * @return an ID uniquely identifying this HeapFile.
     */
    public int getId() {
        // some code goes here
        return f.getAbsoluteFile().hashCode();
    }

    /**
     * Returns the TupleDesc of the table stored in this DbFile.
     * 
     * @return TupleDesc of this DbFile.
     */
    public TupleDesc getTupleDesc() {
        // some code goes here
        return td;
    }

    // see DbFile.java for javadocs
    public Page readPage(PageId pid) {
        // some code goes here
    	int pageSize = BufferPool.getPageSize();
    	byte[] data = new byte[pageSize];
    	try {
    		RandomAccessFile rf = new RandomAccessFile(f, "rw");
    		rf.seek(pageSize * pid.getPageNumber());
    		for (int i = 0; i < pageSize; i++) {
    			data[i] = rf.readByte();
    		}
    		rf.close();
    	} catch(Exception e) {
    	}
        try {
        	HeapPage res =  new HeapPage((HeapPageId)pid, data);
        	return res;
        } catch(IOException e) {
        	return null;
        }
    }

    // see DbFile.java for javadocs
    public void writePage(Page page) throws IOException {
        // some code goes here
        // not necessary for lab1
    	byte[] data = page.getPageData();
    	int pageSize = BufferPool.getPageSize();
    	try {
    		RandomAccessFile rf = new RandomAccessFile(f, "rw");
    		rf.seek(pageSize * page.getId().getPageNumber());
    		rf.write(data);
    		rf.close();
    	} catch(Exception e) {
    	}
    }

    /**
     * Returns the number of pages in this HeapFile.
     */
    public int numPages() {
        // some code goes here
        return (int)Math.ceil(f.length() / BufferPool.getPageSize());
    }

    // see DbFile.java for javadocs
    public ArrayList<Page> insertTuple(TransactionId tid, Tuple t)
            throws DbException, IOException, TransactionAbortedException {
        // some code goes here
        if (!td.equals(t.getTupleDesc())) {
        	throw new DbException("Tuple can not be added!");
        }
        ArrayList<Page> res = new ArrayList<Page>();
    	for (int i = 0; i < numPages(); i++) {
    		try {
    			HeapPage p = (HeapPage)Database.getBufferPool().getPage(tid, new HeapPageId(getId(), i), 
    					Permissions.READ_WRITE);
    			p.insertTuple(t);
    			res.add(p);
    			return res;
    		} catch (Exception e) {}
    	}
    	try {
    		RandomAccessFile rf = new RandomAccessFile(f, "rw");
    		rf.setLength(rf.length() + BufferPool.getPageSize());
    		HeapPage p = (HeapPage)Database.getBufferPool().getPage(tid, new HeapPageId(getId(), numPages()-1), 
					Permissions.READ_WRITE);
    		p.insertTuple(t);
    		res.add(p);
    		rf.close();
    	} catch (Exception e) {
    	}
    	return res;
    }

    // see DbFile.java for javadocs
    public ArrayList<Page> deleteTuple(TransactionId tid, Tuple t) throws DbException,
            TransactionAbortedException {
        // some code goes here
    	ArrayList<Page> res = new ArrayList<Page>();
		try {
			HeapPage p = (HeapPage)Database.getBufferPool().getPage(tid, t.getRecordId().getPageId(), 
					Permissions.READ_WRITE);
			p.deleteTuple(t);
			res.add(p);
			return res;
		} catch (Exception e) {}
    	if (res.isEmpty()) {
    		throw new DbException("This tuple can't be deleted!");
    	}
    	t.setRecordId(null);
    	return res;
    }

    // see DbFile.java for javadocs
    public DbFileIterator iterator(TransactionId tid) {
        // some code goes here
    	return new DbFileIterator() {
    		private int i;
    		private Iterator<Tuple> current;
    		public void open()
    		        throws DbException, TransactionAbortedException {
    			i = 0;
    			current = ((HeapPage)Database.getBufferPool().getPage(tid, new HeapPageId(getId(), i), 
    					Permissions.READ_ONLY)).iterator();
    		}
    		
	    	public boolean hasNext() 
	    			throws DbException, TransactionAbortedException {
	    		if (current == null) {
	    			return false;
	    		}
	    		while (true) {
		    		if (current.hasNext()) {
		    			return true;
		    		}
	    			i++;
	    			if (i >= numPages()) {
	    				return false;
	    			}
	    			current = ((HeapPage)Database.getBufferPool().getPage(tid, new HeapPageId(getId(), i), 
	    					Permissions.READ_ONLY)).iterator();
	    		}
	    	}
	    	public Tuple next()
	    	        throws DbException, TransactionAbortedException, NoSuchElementException {
        		if (!hasNext()) throw new NoSuchElementException();
        		return current.next();
	    	}
	    	public void rewind() throws DbException, TransactionAbortedException {
	    		
	    	}
	    	public void close() {
	    		current = null;
	    	}
    	};
    }

}

