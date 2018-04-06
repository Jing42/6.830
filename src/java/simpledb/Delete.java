package simpledb;

import java.io.IOException;

/**
 * The delete operator. Delete reads tuples from its child operator and removes
 * them from the table they belong to.
 */
public class Delete extends Operator {

    private static final long serialVersionUID = 1L;
    private TransactionId t;
    private OpIterator child;
    
    private boolean run;

    /**
     * Constructor specifying the transaction that this delete belongs to as
     * well as the child to read from.
     * 
     * @param t
     *            The transaction this delete runs in
     * @param child
     *            The child operator from which to read tuples for deletion
     */
    public Delete(TransactionId t, OpIterator child) {
        // some code goes here
    	this.t = t;
    	this.child = child;
    	run = true;
    }

    public TupleDesc getTupleDesc() {
        // some code goes here
    	return new TupleDesc(new Type[]{Type.INT_TYPE});
    }

    public void open() throws DbException, TransactionAbortedException {
        // some code goes here
    	super.open();
    	child.open();
    	run = false;
    }

    public void close() {
        // some code goes here
    	child.close();
    	super.close();
    	run = true;
    }

    public void rewind() throws DbException, TransactionAbortedException {
        // some code goes here
    	child.rewind();
    	run = false;
    }

    /**
     * Deletes tuples as they are read from the child operator. Deletes are
     * processed via the buffer pool (which can be accessed via the
     * Database.getBufferPool() method.
     * 
     * @return A 1-field tuple containing the number of deleted records.
     * @see Database#getBufferPool
     * @see BufferPool#deleteTuple
     */
    protected Tuple fetchNext() throws TransactionAbortedException, DbException {
    	if (run) return null;
    	int res = 0;
    	while (child.hasNext()) {
	    	try {
				Database.getBufferPool().deleteTuple(t, child.next());
				res++;
			} catch (Exception e) {		
			} 
    	}
    	Tuple t = new Tuple(getTupleDesc());
    	t.setField(0, new IntField(res));
    	run = true;
    	//System.out.println(t.getTupleDesc());
    	//System.out.println(t);
        return t ;
    }

    @Override
    public OpIterator[] getChildren() {
        // some code goes here
    	return new OpIterator[] {child};
    }

    @Override
    public void setChildren(OpIterator[] children) {
        // some code goes here
    	child = children[0];
    }

}
