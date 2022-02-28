package simpledb;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

/**
 * Knows how to compute some aggregate over a set of IntFields.
 */
public class IntegerAggregator implements Aggregator {

    private static final long serialVersionUID = 1L;
    private int gbField;
    private Type gbFieldType;
    private int aField;
    private Op aOp;
    
    private HashMap<Field, Integer> aggregate;
    private HashMap<Field, Integer> fieldCount;
    
    private String aggrFieldName;

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
        this.gbField = gbfield;
        this.gbFieldType = gbfieldtype;
        this.aField = afield;
        this.aOp = what;
        
        this.aggregate = new HashMap<Field, Integer>();
        this.fieldCount = new HashMap<Field, Integer>();
        
        this.aggrFieldName = "agName(" + what.toString() + ") (";
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
        // Get the value of the current GROUP BY field
        Field curGbField;
        if (this.gbField == NO_GROUPING) {
            curGbField = new IntField(NO_GROUPING);
        } else {
            curGbField = tup.getField(this.gbField);
        }
        
        // Increase the count for current field
        this.fieldCount.putIfAbsent(curGbField, 0);
        this.fieldCount.put(curGbField, this.fieldCount.get(curGbField)+1);
        
        // Finish naming the aggregate column
        if (!this.aggrFieldName.endsWith(")")) {
            this.aggrFieldName+=tup.getTupleDesc().getFieldName(this.aField) + ")";
        }
        
        // Add into aggregate according to the operator
        int curValue;

        switch (this.aOp.toString()) {
        case "min":
            curValue = this.aggregate.getOrDefault(curGbField, Integer.MAX_VALUE);
            this.aggregate.put(curGbField, Math.min(curValue, ((IntField) tup.getField(this.aField)).getValue()));
            break;
        case "max":
            curValue = this.aggregate.getOrDefault(curGbField, Integer.MIN_VALUE);
            this.aggregate.put(curGbField, Math.max(curValue, ((IntField) tup.getField(this.aField)).getValue()));
            break;
        default:
            curValue = this.aggregate.getOrDefault(curGbField, 0);
            this.aggregate.put(curGbField, curValue + ((IntField) tup.getField(this.aField)).getValue());
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
            switch (this.aOp.toString()) {
            case "avg":
                curTuple.setField(0, new IntField(this.aggregate.get(curField) / this.fieldCount.get(curField)));
                break;
            case "count":  
                curTuple.setField(0, new IntField(this.fieldCount.get(curField)));
                break;
            default:
                curTuple.setField(0, new IntField(this.aggregate.get(curField)));
            }

            result.add(curTuple);
            
        } else {
            for (Field curField : this.aggregate.keySet()) {
                Tuple curTuple = new Tuple(resultDesc);
                curTuple.setField(0, curField);
                switch (this.aOp.toString()) {
                case "avg":
                    curTuple.setField(1, new IntField(this.aggregate.get(curField) / this.fieldCount.get(curField)));
                    break;
                case "count":  
                    curTuple.setField(1, new IntField(this.fieldCount.get(curField)));
                    break;
                default:
                    curTuple.setField(1, new IntField(this.aggregate.get(curField)));
                }

                result.add(curTuple);
            }
        }
        
        return new TupleIterator(resultDesc, result);
        
    }

}
