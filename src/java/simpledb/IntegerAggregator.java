package simpledb;

import java.util.ArrayList;
import java.util.List;
import java.util.NoSuchElementException;

/**
 * Knows how to compute some aggregate over a set of IntFields.
 */
public class IntegerAggregator implements Aggregator {

    private static final long serialVersionUID = 1L;

    private int gbfield, afield;
    private Type gbfieldtype;
    private Op what;
    private List<Pair> contents;
    
    class Pair {
    	Field gpb;
    	int res;
    	int count;
    	int sum;
    	
    	Pair(Field a1) {
    		gpb = a1;
    		res = Integer.MAX_VALUE;
    		count = 0;
    		sum = 0;
    	}
    }
    /**
     * Aggregate constructor
     * 
     * @param gbfield
     *            the 0-based index of the group-by field in the tuple, or
     *            NO_GROUPING if there is no grouping
     * @param gbfieldtype
     *            the type of the group by field (e.g., Type.INT_TYPE), or null
     *            if there is no grouping
     * @param afield
     *            the 0-based index of the aggregate field in the tuple
     * @param what
     *            the aggregation operator
     */

    public IntegerAggregator(int gbfield, Type gbfieldtype, int afield, Op what) {
        // some code goes here
    	this.gbfield = gbfield;
    	this.afield = afield;
    	this.what = what;
    	this.gbfieldtype = gbfieldtype;
    	this.contents = new ArrayList<Pair>();
    }

    /**
     * Merge a new tuple into the aggregate, grouping as indicated in the
     * constructor
     * 
     * @param tup
     *            the Tuple containing an aggregate field and a group-by field
     */
    public void mergeTupleIntoGroup(Tuple tup) {
        // some code goes here
    	int idx = 0;
    	int target = ((IntField)tup.getField(afield)).getValue();
    	if (gbfield != NO_GROUPING) {
    		Field gp = tup.getField(gbfield);
    		for (Pair i: contents) {
        		if (i.gpb.equals(gp)) {
        			break;
        		}
        		idx++;
        	}
        	if (idx == contents.size()) {
        		contents.add(new Pair(gp));
        	}
    	} else {
    		if (contents.size() == 0) {
    			contents.add(new Pair(null));
    		}
    	}
    	Pair p = contents.get(idx);
    	p.count++;
    	p.sum += target;
    	if (p.res == Integer.MAX_VALUE) {
    		p.res = target;
    		return;
    	}
    	switch(what) {
    	case MIN:
    		if (p.res > target) {
    			p.res = target;
    		}
    		break;
    	case MAX:
    		if (p.res < target) {
    			p.res = target;
    		}
    		break;
    	case SUM:
    		p.res += target;
    		break;
    	case COUNT:
    		p.res = p.count;
    		System.out.println("fjdskl");
    		break;
    	case AVG:
    		p.res = p.sum / p.count;
    		break;
    	}
    	
    		
    }

    /**
     * Create a OpIterator over group aggregate results.
     * 
     * @return a OpIterator whose tuples are the pair (groupVal, aggregateVal)
     *         if using group, or a single (aggregateVal) if no grouping. The
     *         aggregateVal is determined by the type of aggregate specified in
     *         the constructor.
     */
    public OpIterator iterator() {
        // some code goes here
        return new OpIterator() {
        	int i = -1;
        	public void open()
        		      throws DbException, TransactionAbortedException {
        		i = 0;
        		
        	}
        	public boolean hasNext() throws DbException, TransactionAbortedException {
        		if (i < 0 || i >= contents.size()) {
        			return false;
        		}
        		return true;
        	}
        	public Tuple next() throws DbException, TransactionAbortedException, NoSuchElementException {
        		if (!hasNext()) throw new NoSuchElementException();
        		Tuple res = new Tuple(getTupleDesc());
        		if (gbfield != NO_GROUPING) {
	        		res.setField(0, contents.get(i).gpb);
	        		res.setField(1, new IntField(contents.get(i).res));
        		} else {
        			res.setField(0, new IntField(contents.get(i).res));
        		}
        		i++;
        		return res;
        	}
        	public void rewind() throws DbException, TransactionAbortedException {
        		i = 0;
        	}
        	public TupleDesc getTupleDesc() {
        		if (gbfield != NO_GROUPING) {
        			return new TupleDesc(new Type[]{gbfieldtype, Type.INT_TYPE});
        		} else {
        			return new TupleDesc(new Type[] {Type.INT_TYPE});
        		}
        	}
        	public void close() {
        		i = -1;
        	}
        };
    }

}
