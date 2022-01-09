package simpledb.optimizer;

import simpledb.execution.Predicate;

import java.util.concurrent.ConcurrentHashMap;

/** A class to represent a fixed-width histogram over a single integer-based field.
 */
public class IntHistogram {
    /*The number of buckets to split the input value into.*/
    private int buckets;
    /*The maximum integer value that will ever be passed to this class for histogramming*/
    private int max;
    /*The minimum integer value that will ever be passed to this class for histogramming*/
    private int min;
    private double width;

    private int count;


    private int[] histogram;

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
        this.buckets = buckets;
        this.min = min;
        this.max = max;
        this.width = (max - min + 1) * 1.0 / buckets;

        histogram = new int[buckets];
    }

    /**
     * Add a value to the set of values that you are keeping a histogram of.
     * @param v Value to add to the histogram
     */
    public void addValue(int v) {
    	// some code goes here
        count ++;
        histogram[getBucketsIndex(v)] ++;
    }

    private int getBucketsIndex(int v) {
        return (int) ((v - min) / width);
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
        double cnt = 0;
        int idx = getBucketsIndex(v);
        int start = (int) Math.ceil(idx * width) + min;
        int end = (int) Math.ceil((idx+1) * width - 1) + min;
        switch (op) {
            case GREATER_THAN:
                if (v < min) return 1.0;
                else if (v >= max) return 0.0;
                for (int i = idx + 1; i < buckets; i ++) {
                    cnt += histogram[i];
                }
                cnt += histogram[idx] * 1.0 /(end - start + 1) * (end - v);
                break;
            case EQUALS:
                if (v < min || v > max) return 0.0;
                cnt = histogram[idx] * 1.0 /(end - start + 1);
                break;
            case LESS_THAN:
                if (v <= min) return 0.0;
                else if (v > max) return 1.0;
                for (int i = 0; i < idx; i ++) {
                    cnt += histogram[i];
                }
                cnt += histogram[idx] * 1.0 /(end - start + 1) * (v - start);
                break;
            case LESS_THAN_OR_EQ:
                if (v < min) return 0.0;
                else if (v >= max) return 1.0;
                for (int i = 0; i < idx; i ++) {
                    cnt += histogram[i];
                }
                cnt += histogram[idx] * 1.0 /(end - start + 1) * (v - start + 1);
                break;
            case GREATER_THAN_OR_EQ:
                if (v <= min) return 1.0;
                else if (v > max) return 0.0;
                for (int i = idx + 1; i < buckets; i ++) {
                    cnt += histogram[i];
                }
                cnt += histogram[idx] * 1.0 /(end - start + 1) * (end - v + 1);
                break;
            case NOT_EQUALS:
                if (v < min || v > max) return 1.0;
                cnt = count - histogram[idx] * 1.0 /(end - start + 1);
                break;
            default:
                throw new IllegalStateException("Unexpected value: " + op);
        }
        return cnt/count;
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
        StringBuffer sb = new StringBuffer();
        sb.append("count: [ ");
        for (int i = 0; i < buckets; i++) {
            if (i != 0)
                sb.append(", ");
            sb.append("bucket ").append(i)
                    .append(" ").append(histogram[i]);
        }
        sb.append(" ]");
        return sb.toString();
    }
}
