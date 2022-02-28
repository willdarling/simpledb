package simpledb;

import java.util.ArrayList;
import java.util.HashMap;

import simpledb.Aggregator.Op;

/**
 * Knows how to compute some aggregate over a set of StringFields.
 */
public class StringAggregator implements Aggregator {

    private static final long serialVersionUID = 1L;
    private int aField;
    private int gbField;
    private Type gbFieldType;

    private HashMap<Field, Integer> fieldCount;
    
    private String aggrFieldName;

    /**
     * Aggregate constructor
     * @param gbfield the 0-based index of the group-by field in the tuple, or NO_GROUPING if there is no grouping
     * @param gbfieldtype the type of the group by field (e.g., Type.INT_TYPE), or null if there is no grouping
     * @param afield the 0-based index of the aggregate field in the tuple
     * @param what aggregation operator to use -- only supports COUNT
     * @throws IllegalArgumentException if what != COUNT
     */

    public StringAggregator(int gbfield, Type gbfieldtype, int afield, Op what) {
        // some code goes here
        if (!what.toString().equals("count")) {
            throw new IllegalArgumentException("Tried to use string aggregator with operator other than COUNT");
        }
        
        this.aField = afield;
        this.gbField = gbfield;
        this.gbFieldType = gbfieldtype;

        this.fieldCount = new HashMap<Field, Integer>();
        
        this.aggrFieldName = "agName(" + what.toString() + ") (";
    }

    /**
     * Merge a new tuple into the aggregate, grouping as indicated in the constructor
     * @param tup the Tuple containing an aggregate field and a group-by field
     */
    public void mergeTupleIntoGroup(Tuple tup) {
        // some code goes here
        // Get the value of the current GROUP BY field
        Field curGbField;
        if (this.gbField == NO_GROUPING) {
            curGbField = new IntField(NO_GROUPING);
        } else {
            curGbField = tup.getField(this.gbField);
        }
        
        // Finish naming the aggregate column
        if (!this.aggrFieldName.endsWith(")")) {
            this.aggrFieldName+=tup.getTupleDesc().getFieldName(this.aField) + ")";
        }
        
        // Increase the count for current field
        this.fieldCount.putIfAbsent(curGbField, 0);
        this.fieldCount.put(curGbField, this.fieldCount.get(curGbField)+1);
    }

    /**
     * Create a OpIterator over group aggregate results.
     *
     * @return a OpIterator whose tuples are the pair (groupVal,
     *   aggregateVal) if using group, or a single (aggregateVal) if no
     *   grouping. The aggregateVal is determined by the type of
     *   aggregate specified in the constructor.
     */
    public OpIterator iterator() {
        // some code goes here
        ArrayList<Tuple> result = new ArrayList<Tuple>();
        TupleDesc resultDesc;
        Type[] resultTypeAr;
        String[] resultFieldAr;
        
        // Constructing TupleDesc for aggregate results
        if (this.gbField == NO_GROUPING) {
            resultTypeAr = new Type[] {Type.INT_TYPE};
            resultFieldAr = new String[] {this.aggrFieldName};
        } else {
            resultTypeAr = new Type[] {this.gbFieldType, Type.INT_TYPE};
            resultFieldAr = new String[] {"groupVal", this.aggrFieldName};
        }
        resultDesc = new TupleDesc(resultTypeAr, resultFieldAr);
        
        // Constructing a list of tuples
        if (this.gbField == NO_GROUPING) {
            Tuple curTuple = new Tuple(resultDesc);
            IntField curField = new IntField(NO_GROUPING);
            curTuple.setField(0, new IntField(this.fieldCount.get(curField)));
            result.add(curTuple);
            
        } else {
            for (Field curField : this.fieldCount.keySet()) {
                Tuple curTuple = new Tuple(resultDesc);
                curTuple.setField(0, curField); 
                curTuple.setField(1, new IntField(this.fieldCount.get(curField)));
                result.add(curTuple);
            }
        }
        
        return new TupleIterator(resultDesc, result);
    }

}
