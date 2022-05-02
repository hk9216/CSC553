package simpledb;
import java.io.*;
/**
 * Inserts tuples read from the child operator into the tableid specified in the
 * constructor
 */
public class Insert extends Operator {

    private static final long serialVersionUID = 1L;
     private int tableid;
    private DbIterator child;
    private DbIterator[] children;
    private TransactionId tid;
    private TupleDesc td;
    private boolean called;
    /**
     * Constructor.
     * 
     * @param t
     *            The transaction running the insert.
     * @param child
     *            The child operator from which to read tuples to be inserted.
     * @param tableid
     *            The table in which to insert tuples.
     * @throws DbException
     *             if TupleDesc of child differs from table into which we are to
     *             insert.
     */
    public Insert(TransactionId t,DbIterator child, int tableid)
            throws DbException {
        // some code goes here
        this.tableid = tableid;
        this.child = child;
        this.tid = t;
        this.children = new DbIterator[1];
    	this.children[0] = child;
        this.td = Utility.getTupleDesc(1);
        called = false;
        if (!child.getTupleDesc().equals(
                Database.getCatalog().getTupleDesc(tableid))) {
            throw new DbException("TupleDesc mismatch");
        }
    }

    public TupleDesc getTupleDesc() {
        // some code goes here
        return td;
    }

    public void open() throws DbException, TransactionAbortedException {
        // some code goes here
        super.open();
        child.open();
    }

    public void close() {
        // some code goes here
       child.close();
        super.close();
    }

    public void rewind() throws DbException, TransactionAbortedException {
        // some code goes here
        child.rewind();
    }

    /**
     * Inserts tuples read from child into the tableid specified by the
     * constructor. It returns a one field tuple containing the number of
     * inserted records. Inserts should be passed through BufferPool. An
     * instances of BufferPool is available via Database.getBufferPool(). Note
     * that insert DOES NOT need check to see if a particular tuple is a
     * duplicate before inserting it.
     * 
     * @return A 1-field tuple containing the number of inserted records, or
     *         null if called more than once.
     * @see Database#getBufferPool
     * @see BufferPool#insertTuple
     */
    protected Tuple fetchNext() throws TransactionAbortedException, DbException {
        // some code goes here
         if (called) {
            return null;
        }
        called = true;
        int count = 0;
        BufferPool bp = Database.getBufferPool();
        while (child.hasNext()) {
            Tuple t = child.next();
            try {
                bp.insertTuple(tid, tableid, t);
                count++;
            } catch (IOException e) {
               // e.printStackTrace();
                System.exit(-1);
            }
        }
        return Utility.getTuple(new int[] { count }, 1);
    }

    @Override
    public DbIterator[] getChildren() {
        // some code goes here
        return children;
    }

    @Override
    public void setChildren(DbIterator[] children) {
        // some code goes here
       this.children=children;
    }
}
