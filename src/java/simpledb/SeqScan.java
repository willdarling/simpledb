package simpledb;

import java.util.*;


/**
 * SeqScan is an implementation of a sequential scan access method that reads
 * each tuple of a table in no particular order (e.g., as they are laid out on
 * disk).
 */
public class SeqScan implements OpIterator {

    private static final long serialVersionUID = 1L;
    private TransactionId tId;
    private int tableId;
    private String alias;
    private DbFileIterator tableIter;
    
    private boolean iterOpen;

    /**
     * Creates a sequential scan over the specified table as a part of the
     * specified transaction.
     *
     * @param tid
     *            The transaction this scan is running as a part of.
     * @param tableid
     *            the table to scan.
     * @param tableAlias
     *            the alias of this table (needed by the parser); the returned
     *            tupleDesc should have fields with name tableAlias.fieldName
     *            (note: this class is not responsible for handling a case where
     *            tableAlias or fieldName are null. It shouldn't crash if they
     *            are, but the resulting name can be null.fieldName,
     *            tableAlias.null, or null.null).
     */
    public SeqScan(TransactionId tid, int tableid, String tableAlias) {
        // some code goes here
        this.tId = tid;
        this.tableId = tableid;
        this.alias = tableAlias;
        this.tableIter = null;
    }

    /**
     * @return
     *       return the table name of the table the operator scans. This should
     *       be the actual name of the table in the catalog of the database
     * */
    public String getTableName() {
        return Database.getCatalog().getTableName(this.tableId);
    }

    /**
     * @return Return the alias of the table this operator scans.
     * */
    public String getAlias()
    {
        // some code goes here
        return this.alias;
    }

    /**
     * Reset the tableid, and tableAlias of this operator.
     * @param tableid
     *            the table to scan.
     * @param tableAlias
     *            the alias of this table (needed by the parser); the returned
     *            tupleDesc should have fields with name tableAlias.fieldName
     *            (note: this class is not responsible for handling a case where
     *            tableAlias or fieldName are null. It shouldn't crash if they
     *            are, but the resulting name can be null.fieldName,
     *            tableAlias.null, or null.null).
     */
    public void reset(int tableid, String tableAlias) {
        // some code goes here
        this.tableId = tableid;
        this.alias = tableAlias;
    }

    public SeqScan(TransactionId tid, int tableId) {
        this(tid, tableId, Database.getCatalog().getTableName(tableId));
    }

    public void open() throws DbException, TransactionAbortedException {
        // some code goes here
        this.tableIter = Database.getCatalog().getDatabaseFile(this.tableId).iterator(this.tId);
        this.tableIter.open();
        this.iterOpen = true;
    }

    /**
     * Returns the TupleDesc with field names from the underlying HeapFile,
     * prefixed with the tableAlias string from the constructor. This prefix
     * becomes useful when joining tables containing a field(s) with the same
     * name.  The alias and name should be separated with a "." character
     * (e.g., "alias.fieldName").
     *
     * @return the TupleDesc with field names from the underlying HeapFile,
     *         prefixed with the tableAlias string from the constructor.
     */
    public TupleDesc getTupleDesc() {
        // some code goes here
        TupleDesc prevDesc = Database.getCatalog().getDatabaseFile(this.tableId).getTupleDesc();
        
        // Making two arrays to store new types and filednames
        ArrayList<Type> newTypeAr = new ArrayList<Type>();
        ArrayList<String> newFieldAr = new ArrayList<String>();
        
        // Filling the arraylists with types and names
        prevDesc.iterator().forEachRemaining(tup -> {
            newTypeAr.add(tup.fieldType);
            newFieldAr.add(this.alias+"."+tup.fieldName);
        });
        
        // Converting arraylists to array so we can pass them to constructor
        Type[] typeRes = new Type[newTypeAr.size()];
        newTypeAr.toArray(typeRes);
        
        String[] nameRes = new String[newFieldAr.size()];;
        newFieldAr.toArray(nameRes);
        
        return new TupleDesc(typeRes, nameRes);
    }

    public boolean hasNext() throws TransactionAbortedException, DbException {
        // some code goes here
        if (!this.iterOpen) {
            throw new IllegalStateException("Iterator hasn't been opened yet");
        }
        
        return this.tableIter.hasNext();
    }

    public Tuple next() throws NoSuchElementException,
            TransactionAbortedException, DbException {
        // some code goes here
        if (!this.hasNext()) {
            throw new NoSuchElementException("There are no more tuples to fetch");
        }
        Tuple result = this.tableIter.next();
        
        return result;
    }

    public void close() {
        // some code goes here
        if (this.iterOpen) {
            this.tableIter.close();
            this.tableIter = null;
            this.iterOpen = false;
        }
    }

    public void rewind() throws DbException, NoSuchElementException,
            TransactionAbortedException {
        // some code goes here
        this.close();
        this.open();
    }
}
