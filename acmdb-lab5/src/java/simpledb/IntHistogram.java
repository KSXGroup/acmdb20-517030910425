package simpledb;

/** A class to represent a fixed-width histogram over a single integer-based field.
 */
public class IntHistogram {

    private final int numBuckets;
    private final int min;
    private final int max;
    private final int width;
    private final int[] bucket;
    private int numTuples;

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
    	// some code goes here
        this.numBuckets = buckets;
        this.numTuples = 0;
        this.max = max;
        this.min = min;
        this.width = (max - min + buckets) / buckets;
        this.bucket = new int[buckets];
        for(int i = 0; i < buckets; ++i) this.bucket[i] = 0;
    }

    /**
     * Add a value to the set of values that you are keeping a histogram of.
     * @param v Value to add to the histogram
     */
    public void addValue(int v) {
    	// some code goes here
        //System.out.println((v - min) / width);
        this.bucket[(v - this.min) / this.width] += 1;
        this.numTuples += 1;
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

    	// some code goes here
        int idx = (v - this.min) / this.width;
        double res = 0.0;
        int tmpsum;
        switch (op){
            case EQUALS:
            case NOT_EQUALS:
                if(idx < 0 || idx > this.numBuckets - 1) {
                    if(op.equals(Predicate.Op.NOT_EQUALS))
                        res = 1.0;
                    else
                        res = 0.0;
                }
                else {
                    res = this.bucket[idx] / (double) (this.width * this.numTuples);
                    if (op.equals(Predicate.Op.NOT_EQUALS)) res = 1.0 - res;
                }
                break;
            case LESS_THAN_OR_EQ:
            case GREATER_THAN:
                if(idx < 0) res = 0.0;
                else if(idx > this.numBuckets - 1) res = 1.0;
                else {
                    tmpsum = 0;
                    for (int i = 0; i < idx; ++i) tmpsum += this.bucket[i];
                    tmpsum += ((v - this.min - idx * this.width + 1) / (double)(this.width)) * (this.bucket[idx]);
                    res = tmpsum / (double) this.numTuples;
                }
                if(op.equals(Predicate.Op.GREATER_THAN)) res = 1.0 - res;
                break;
            case GREATER_THAN_OR_EQ:
            case LESS_THAN:
                if(idx < 0) res = 0.0;
                else if(idx > this.numBuckets - 1) res = 1.0;
                else {
                    tmpsum = 0;
                    for (int i = 0; i < idx; ++i) tmpsum += this.bucket[i];
                    tmpsum += ((v - this.min - idx * this.width) / (double)(this.width)) * (this.bucket[idx]);
                    res = tmpsum / (double) this.numTuples;
                }
                if(op.equals(Predicate.Op.GREATER_THAN_OR_EQ)) res = 1.0 - res;
                break;
        }
        return res;
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
        // some code goes here
        return 1.0;
    }
    
    /**
     * @return A string describing this histogram, for debugging purposes
     */
    public String toString() {
        // some code goes here
        return "min: " + this.min + " max: " + this.max + "width: "+ this.width + "\n";
    }
}
