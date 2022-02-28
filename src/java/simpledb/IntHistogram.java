package simpledb;

/** A class to represent a fixed-width histogram over a single integer-based field.
 */
public class IntHistogram {

    private int[] buckets;
    private int min;
    private int max;
    private int bucketNum;
    private double bucketSize;
    private int tuples;

    /**
     * Create a new IntHistogram.
     * 
     * This IntHistogram should maintain a histogram of integer values that it receives.
     * It should split the histogram into "buckets" buckets.
     * 
     * The values that are being histogrammed will be provided one-at-a-time through the "addValue()" function.
     * 
     * Your implementation should use space and have execution time that are both
     * constant with respect to the number of values being histogrammed.  For example, you shouldn't 
     * simply store every value that you see in a sorted list.
     * 
     * @param buckets The number of buckets to split the input value into.
     * @param min The minimum integer value that will ever be passed to this class for histogramming
     * @param max The maximum integer value that will ever be passed to this class for histogramming
     */
    public IntHistogram(int buckets, int min, int max) {
    	this.bucketNum = buckets;
        this.min = min;
        this.max = max;
        this.buckets = new int[buckets];
        this.bucketSize = (max-min) / (double) buckets;
        this.tuples = 0;
    }

    private int index(int v) {
        int index = (int) ((v-min) / bucketSize);
        if (index == this.bucketNum) {
            index--;
        }
        return index;
    }

    /**
     * Add a value to the set of values that you are keeping a histogram of.
     * @param v Value to add to the histogram
     */
    public void addValue(int v) {
    	this.buckets[index(v)]++;
        tuples++;
    }

    private double estimateEq(int v) {
        if (v < min || v > max)
            return 0.0;

        return (buckets[index(v)] / Math.max(1, bucketSize)) / tuples;
    }

    private double estimateGt(int v) {
        if (v < min) 
            return 1.0;
        if (v > max)
            return 0.0;
        
        int index = index(v);
        double b_right = (index + 1) * bucketSize;
        double b_part = (b_right - v) / bucketSize;

        double b_fs = 0.0;
        for (int i=index+1; i<bucketNum; i++)
            b_fs += (double) buckets[i]/tuples;

        return (buckets[index]/tuples) * b_part + b_fs;
    }

    /**
     * Estimate the selectivity of a particular predicate and operand on this table.
     * 
     * For example, if "op" is "GREATER_THAN" and "v" is 5, 
     * return your estimate of the fraction of elements that are greater than 5.
     * 
     * @param op Operator
     * @param v Value
     * @return Predicted selectivity of this particular operator and value
     */
    public double estimateSelectivity(Predicate.Op op, int v) {
        double selectivity = 0.0;

        if (op == Predicate.Op.EQUALS)
            selectivity = estimateEq(v);
        else if (op == Predicate.Op.GREATER_THAN)
            selectivity = estimateGt(v);
        else if (op == Predicate.Op.LESS_THAN)
            selectivity = 1.0 - estimateGt(v) - estimateEq(v);
        else if (op == Predicate.Op.LESS_THAN_OR_EQ)
            selectivity = 1.0 - estimateGt(v);
        else if (op == Predicate.Op.GREATER_THAN_OR_EQ)
            selectivity = estimateGt(v) + estimateEq(v);
        else if (op == Predicate.Op.LIKE)
            selectivity = estimateEq(v);
        else if (op == Predicate.Op.NOT_EQUALS)
            selectivity = 1.0 - estimateEq(v);

        return selectivity;
    }
    
    /**
     * @return
     *     the average selectivity of this histogram.
     *     
     *     This is not an indispensable method to implement the basic
     *     join optimization. It may be needed if you want to
     *     implement a more efficient optimization
     * */
    public double avgSelectivity()
    {
        /*
        int rows = 0;
        for (int i=0; i<bucketNum; i++) {
            rows += buckets[i];
        }
        return rows/(double) tuples;
        */
        return -1.0;
    }
    
    /**
     * @return A string describing this histogram, for debugging purposes
     */
    public String toString() {
        String str = "min: "+Integer.toString(min)+" max: "+Integer.toString(max)
                     +"\ntuples: "+Integer.toString(tuples)+" bucketNum: "+Integer.toString(bucketNum)
                     +" bucketSize: "+Double.toString(bucketSize)+"\n(";
        for (int i=0; i<bucketNum; i++) {
            str += Integer.toString(buckets[i])+" ";
        }
        str += ")";
        return str;
    }
}
