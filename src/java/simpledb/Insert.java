package simpledb;

import java.io.IOException;
import java.util.NoSuchElementException;

//import com.sun.org.apache.bcel.internal.generic.IfInstruction;

/**
 * Inserts tuples read from the child operator into the tableId specified in the
 * constructor
 */
public class Insert extends Operator {

    private static final long serialVersionUID = 1L;
    private TransactionId tId;
    private OpIterator child;
    private int tableId;
    private boolean fetchCalled;
   

    /**
     * Constructor.
     *
     * @param t
     *            The transaction running the insert.
     * @param child
     *            The child operator from which to read tuples to be inserted.
     * @param tableId
     *            The table in which to insert tuples.
     * @throws DbException
     *             if TupleDesc of child differs from table into which we are to
     *             insert.
     */
    public Insert(TransactionId t, OpIterator child, int tableId)
            throws DbException {
        // some code goes here
        if (!child.getTupleDesc().equals(Database.getCatalog().getDatabaseFile(tableId).getTupleDesc())) {
            throw new DbException("Couldn't make an Insert because TupleDescs didn't match");
        }
        
        this.tId = t;
        this.child = child;
        this.tableId = tableId;
        this.fetchCalled = false;
    }

    public TupleDesc getTupleDesc() {
        // some code goes here
        return new TupleDesc(new Type[] {Type.INT_TYPE}, new String[] {"nInserted"});
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
        this.close();
        this.open();
    }

    /**
     * Inserts tuples read from child into the tableId specified by the
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
        // If already called - return null
        if (this.fetchCalled) {
            return null;
        }
        this.fetchCalled = true;
        
        // Insert all the tuples from child
        int nInserted = 0;
        try {
            while (this.child.hasNext()) {
                Tuple curTup = this.child.next();
                try {
                    Database.getBufferPool().insertTuple(this.tId, this.tableId, curTup);
                    nInserted++;
                } catch (IOException e) {
                    System.out.println("Couldn't insert next tuple using Insert.fetchnext");
                }
            }
            
            // Make tuple to return
            TupleDesc resultTd = new TupleDesc(new Type[] {Type.INT_TYPE}, new String[] {"nInserted"});
            Tuple result = new Tuple(resultTd);
            result.setField(0, new IntField(nInserted));
            return result;
        } catch (NoSuchElementException | TransactionAbortedException | DbException e) {
            // TODO: handle exception
            System.out.println("Something went wrong in Insert.fetchnext()" );
        }
        
        return null;
        
    }

    @Override
    public OpIterator[] getChildren() {
        // some code goes here
        OpIterator[] result = {this.child};
        return result;
    }

    @Override
    public void setChildren(OpIterator[] children) {
        // some code goes here
        this.child = children[0];
    }
}
