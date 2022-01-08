package simpledb.execution;

import simpledb.common.Type;
import simpledb.storage.*;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Knows how to compute some aggregate over a set of IntFields.
 */
public class IntegerAggregator implements Aggregator {

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

    private ConcurrentHashMap<Field, AggValue> results = new ConcurrentHashMap<>();

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
        this.gbfield = gbfield;
        this.gbfieldtype = gbfieldtype;
        this.afield = afield;
        this.what = what;
        if (gbfield == Aggregator.NO_GROUPING) {
            tupleDesc = new TupleDesc(new Type[]{Type.INT_TYPE});
        } else {
            tupleDesc = new TupleDesc(new Type[]{gbfieldtype, Type.INT_TYPE});
        }
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
        Field key;
        if (gbfield == Aggregator.NO_GROUPING) {
            key = new IntField(-1);
        } else {
            key = tup.getField(gbfield);
        }
        IntField aggf = (IntField) tup.getField(afield);
        AggValue value;
        switch (what) {
            case COUNT:
                value = results.getOrDefault(key, new AggValue(0, 0));
                value.setValue(value.getCount() + 1);
                break;
            case SUM:
            case AVG:
                value = results.getOrDefault(key, new AggValue(0, 0));
                value.setValue(value.value + aggf.getValue());
                break;
            case MAX:
                value = results.getOrDefault(key, new AggValue(Integer.MIN_VALUE, 0));
                value.setValue(Math.max(value.value, aggf.getValue()));
//                value = Math.max(results.getOrDefault(key, Integer.MIN_VALUE), aggf.getValue());
                break;
            case MIN:
                value = results.getOrDefault(key, new AggValue(Integer.MAX_VALUE, 0));
                value.setValue(Math.min(value.value, aggf.getValue()));
                break;
//            case AVG:
//                value = results.getOrDefault(key, new AggValue(0, 0));
//                value.setValue( (value.value * value.count + aggf.getValue()) / (value.count+1));
//                break;
            default:
                throw new IllegalArgumentException("Illegal aggregator " + what.toString());
        }
        value.increaseCount();
        results.put(key, value);
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
        List<Tuple> tupleList = new ArrayList<>(results.size());
        for (Map.Entry<Field, AggValue> entry : results.entrySet()) {
            Tuple t = new Tuple(tupleDesc);
            int aidx = 0;
            if (gbfield != Aggregator.NO_GROUPING) {
                aidx = 1;
                t.setField(0, entry.getKey());
//                t.setField(1, new IntField(entry.getValue().getValue()));
            }
            if (what.equals(Op.AVG)) {
                t.setField(aidx, new IntField( entry.getValue().getValue() / entry.getValue().getCount() ));
            } else {
                t.setField(aidx, new IntField(entry.getValue().getValue()));
            }
            tupleList.add(t);
        }
        return new TupleIterator(tupleDesc, tupleList);
    }

    private class AggValue {
        int count;
        int value;

        public AggValue(int value, int count) {
            this.count = count;
            this.value = value;
        }

        public void increaseCount() {
            count ++;
        }
        public int getCount() {
            return count;
        }
        public int getValue() {
            return value;
        }
        public void setValue(int value) {
            this.value = value;
        }
    }
}
