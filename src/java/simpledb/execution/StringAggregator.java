package simpledb.execution;

import simpledb.common.Type;
import simpledb.storage.*;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Knows how to compute some aggregate over a set of StringFields.
 */
public class StringAggregator implements Aggregator {

    private static final long serialVersionUID = 1L;
    /* the 0-based index of the group-by field in the tuple, or NO_GROUPING if there is no grouping */
    private int gbfield;
    /* the type of the group by field (e.g., Type.INT_TYPE), or null if there is no grouping */
    private Type gbfieldtype;
    /* the 0-based index of the aggregate field in the tuple */
    private int afield;
    /* the aggregation operator */
    private Op what;

    private TupleDesc tupleDesc;

    private ConcurrentHashMap<Field, Integer> result = new ConcurrentHashMap<>();

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
        this.gbfield = gbfield;
        this.gbfieldtype = gbfieldtype;
        this.afield = afield;
        if (!what.equals(Op.COUNT))
            throw new IllegalArgumentException("IllegalArgument aggregate " + what.toString());
        this.what = what;
        if (gbfield == Aggregator.NO_GROUPING) {
            tupleDesc = new TupleDesc(new Type[]{Type.INT_TYPE});
        } else {
            tupleDesc = new TupleDesc(new Type[]{gbfieldtype, Type.INT_TYPE});
        }
    }

    /**
     * Merge a new tuple into the aggregate, grouping as indicated in the constructor
     * @param tup the Tuple containing an aggregate field and a group-by field
     */
    public void mergeTupleIntoGroup(Tuple tup) {
        // some code goes here
        Field key;
        if (gbfield == Aggregator.NO_GROUPING) {
            key = tup.getField(afield);
        } else {
            key = tup.getField(gbfield);
        }
        result.put(key, result.getOrDefault(key, 0) + 1);
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
        List<Tuple> tupleList = new ArrayList<>(result.size());
        for (Map.Entry<Field, Integer> entry : result.entrySet()) {
            Tuple t = new Tuple(tupleDesc);
            if (gbfield == Aggregator.NO_GROUPING) {
                t.setField(0, new IntField(entry.getValue()));
            } else {
                t.setField(0, entry.getKey());
                t.setField(1, new IntField(entry.getValue()));
            }
            tupleList.add(t);
        }
        return new TupleIterator(tupleDesc, tupleList);
    }

}
