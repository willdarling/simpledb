package simpledb;

import java.util.*;

/**
 * The Aggregation operator that computes an aggregate (e.g., sum, avg, max,
 * min). Note that we only support aggregates over a single column, grouped by a
 * single column.
 */
public class Aggregate extends Operator {

    private static final long serialVersionUID = 1L;
    private OpIterator child;
    private int aField;
    private int gbField;
    private Aggregator.Op aOp;
    
    private Aggregator aggr;
    private OpIterator aggr_iter;

    /**
     * Constructor.
     * 
     * Implementation hint: depending on the type of afield, you will want to
     * construct an {@link IntegerAggregator} or {@link StringAggregator} to help
     * you with your implementation of readNext().
     * 
     * 
     * @param child
     *            The OpIterator that is feeding us tuples.
     * @param afield
     *            The column over which we are computing an aggregate.
     * @param gfield
     *            The column over which we are grouping the result, or -1 if
     *            there is no grouping
     * @param aop
     *            The aggregation operator to use
     */
    public Aggregate(OpIterator child, int afield, int gfield, Aggregator.Op aop) {
        // some code goes here
        this.child = child;
        this.aField = afield;
        this.gbField = gfield;
        this.aOp = aop;
        
        // Make the aggregator
        if (child.getTupleDesc().getFieldType(afield) == Type.INT_TYPE) {
            if (gfield == -1) {
                this.aggr = new IntegerAggregator(gfield, null, afield, aop);
            } else {
                this.aggr = new IntegerAggregator(gfield, child.getTupleDesc().getFieldType(gfield), afield, aop);
            }
        } else if (child.getTupleDesc().getFieldType(afield) == Type.STRING_TYPE) {
            if (gfield == -1) {
                this.aggr = new StringAggregator(gfield, null, afield, aop);
            } else {
                this.aggr = new StringAggregator(gfield, child.getTupleDesc().getFieldType(gfield), afield, aop);
            }
        }
        
        // Fill in the aggregator
        try {
            child.open();
            while (child.hasNext()) {
                Tuple curTup = child.next();
                this.aggr.mergeTupleIntoGroup(curTup);
            }
            child.close();
        } catch (DbException | TransactionAbortedException e) {
            // TODO Auto-generated catch block
            System.out.println("Failed while trying to open the child in Aggregate");
        }
        
        // Make the iterator for the aggregator after filling it
        this.aggr_iter = this.aggr.iterator();

    }

    /**
     * @return If this aggregate is accompanied by a groupby, return the groupby
     *         field index in the <b>INPUT</b> tuples. If not, return
     *         {@link simpledb.Aggregator#NO_GROUPING}
     * */
    public int groupField() {
        // some code goes here
        if (this.gbField == -1) {
            return Aggregator.NO_GROUPING;
        }
        return this.gbField;
    }

    /**
     * @return If this aggregate is accompanied by a group by, return the name
     *         of the groupby field in the <b>OUTPUT</b> tuples. If not, return
     *         null;
     * */
    public String groupFieldName() {
    	// some code goes here
        if (this.gbField == -1) {
            return null;
        }
        return this.child.getTupleDesc().getFieldName(0);
    }

    /**
     * @return the aggregate field
     * */
    public int aggregateField() {
    	// some code goes here
    	return this.aField;
    }

    /**
     * @return return the name of the aggregate field in the <b>OUTPUT</b>
     *         tuples
     * */
    public String aggregateFieldName() {
    	// some code goes here
    	return this.child.getTupleDesc().getFieldName(this.aField);
    }

    /**
     * @return return the aggregate operator
     * */
    public Aggregator.Op aggregateOp() {
    	// some code goes here
    	return this.aOp;
    }

    public static String nameOfAggregatorOp(Aggregator.Op aop) {
        return aop.toString();
    }

    public void open() throws NoSuchElementException, DbException,
	    TransactionAbortedException {
        // some code goes here
        super.open();
        this.aggr_iter.open(); 
    }

    /**
     * Returns the next tuple. If there is a group by field, then the first
     * field is the field by which we are grouping, and the second field is the
     * result of computing the aggregate. If there is no group by field, then
     * the result tuple should contain one field representing the result of the
     * aggregate. Should return null if there are no more tuples.
     */
    protected Tuple fetchNext() throws TransactionAbortedException, DbException {
    	// some code goes here
        try {
            while (this.aggr_iter.hasNext()) {
                return this.aggr_iter.next();
            }
        } catch (NoSuchElementException | TransactionAbortedException | DbException e) {
            // TODO: handle exception
            System.out.println("Something went wrong in Aggregate.fetchnext()" );
        }
        return null;
    }

    public void rewind() throws DbException, TransactionAbortedException {
        // some code goes here
        this.close();
        this.open();
    }

    /**
     * Returns the TupleDesc of this Aggregate. If there is no group by field,
     * this will have one field - the aggregate column. If there is a group by
     * field, the first field will be the group by field, and the second will be
     * the aggregate value column.
     * 
     * The name of an aggregate column should be informative. For example:
     * "aggName(aop) (child_td.getFieldName(afield))" where aop and afield are
     * given in the constructor, and child_td is the TupleDesc of the child
     * iterator.
     */
    public TupleDesc getTupleDesc() {
    	// some code goes here
    	return this.aggr_iter.getTupleDesc();
    }

    public void close() {
        // some code goes here
        this.aggr_iter.close();
        super.close();
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
