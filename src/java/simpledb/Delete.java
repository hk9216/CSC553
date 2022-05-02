package simpledb;

import java.io.IOException;
import java.util.NoSuchElementException;
/**
 * The delete operator. Delete reads tuples from its child operator and removes
 * them from the table they belong to.
 */
public class Delete extends Operator {

    private static final long serialVersionUID = 1L;
    TransactionId t;
    DbIterator child;
    DbIterator[] children;
    boolean valid;
    int count;
    TupleDesc td;
    private boolean test;
    /**
     * Constructor specifying the transaction that this delete belongs to as
     * well as the child to read from.
     * 
     * @param t
     *            The transaction this delete runs in
     * @param child
     *            The child operator from which to read tuples for deletion
     */
    public Delete(TransactionId t, DbIterator child) {
        // some code goes here
        this.t = t;
    	this.child = child;
    	this.children = new DbIterator[1];
    	this.children[0] = child;
    	
    	
       
        this.child = child;
        this.t= t;
        this.td = Utility.getTupleDesc(1);
        this.test = false;
    }
    

    public TupleDesc getTupleDesc() {
        // some code goes here
        return this.td;
    }

    public void open() throws DbException, TransactionAbortedException {
        // some code goes here
        super.open();
        this.child.open();
        
    }

    public void close() {
        // some code goes here
        
        this.child.close();
         super.close();
    }

    public void rewind() throws DbException, TransactionAbortedException {
        // some code goes here
        this.child.rewind();
        this.valid = true;
        this.count = 0;
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
    protected Tuple fetchNext() throws DbException, TransactionAbortedException  {
        // some code goes her
     
        if(test==false) return null;
        test= true;
        int count = 0;
        
        while (child.hasNext()) {
            Tuple t1 = child.next();
            Database.getBufferPool().deleteTuple(this.t, t1);
            count++;
        }
        return Utility.getTuple(new int[] { count }, 1);

    
    }

    @Override
    public DbIterator[] getChildren() {
        // some code goes here
        return this.children;
    }

    @Override
    public void setChildren(DbIterator[] children) {
        // some code goes here
        this.children = children;
    }

}
