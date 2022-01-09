package simpledb.optimizer;

import simpledb.execution.Predicate;

public interface Histogram<T> {
//    int maxVal() ;

    /** @return the minimum value indexed by the histogram */
//    int minVal() ;

    /** Add a new value to thte histogram */
    public void addValue(T s);

    /**
     * Estimate the selectivity (as a double between 0 and 1) of the specified
     * predicate over the specified string
     *
     * @param op
     *            The operation being applied
     * @param s
     *            The value to apply op to
     */
    public double estimateSelectivity(Predicate.Op op, T s);

    /**
     * @return the average selectivity of this histogram.
     *
     *         This is not an indispensable method to implement the basic join
     *         optimization. It may be needed if you want to implement a more
     *         efficient optimization
     * */
    public double avgSelectivity();
}
