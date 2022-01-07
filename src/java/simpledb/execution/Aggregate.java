package simpledb.execution;

import simpledb.common.DbException;
import simpledb.common.Type;
import simpledb.storage.Tuple;
import simpledb.storage.TupleDesc;
import simpledb.transaction.TransactionAbortedException;

import java.util.NoSuchElementException;
import java.util.Objects;


/**
 * The Aggregation operator that computes an aggregate (e.g., sum, avg, max,
 * min). Note that we only support aggregates over a single column, grouped by a
 * single column.
 */
public class Aggregate extends Operator {

    private static final long serialVersionUID = 1L;
    /*The OpIterator that is feeding us tuples.*/
    private OpIterator child;
    /*The column over which we are computing an aggregate.*/
    private int afield;
    /*The column over which we are grouping the result, or -1 if there is no grouping*/
    private int gfield;
    /*The aggregation operator to use*/
    private Aggregator.Op aop;

    private Aggregator aggregator;
    private OpIterator aggIterator;

    private TupleDesc tupleDesc;

    /**
     * Constructor.
     * <p>
     * Implementation hint: depending on the type of afield, you will want to
     * construct an {@link IntegerAggregator} or {@link StringAggregator} to help
     * you with your implementation of readNext().
     *
     * @param child  The OpIterator that is feeding us tuples.
     * @param afield The column over which we are computing an aggregate.
     * @param gfield The column over which we are grouping the result, or -1 if
     *               there is no grouping
     * @param aop    The aggregation operator to use
     */
    public Aggregate(OpIterator child, int afield, int gfield, Aggregator.Op aop) {
        // some code goes here
        this.child = child;
        this.afield = afield;
        this.gfield = gfield;
        this.aop = aop;

        TupleDesc td = child.getTupleDesc();
        Type[] typeArr;
        String[] nameArr;
        if (gfield != Aggregator.NO_GROUPING) {
            typeArr = new Type[]{td.getFieldType(gfield), td.getFieldType(afield)};
            nameArr = new String[]{td.getFieldName(gfield), td.getFieldName(afield)};
        } else {
            typeArr = new Type[]{ td.getFieldType(afield) };
            nameArr = new String[] { td.getFieldName(afield) };
        }
        this.tupleDesc = new TupleDesc(typeArr, nameArr);

        if (td.getFieldType(afield).equals(Type.INT_TYPE)) {
            aggregator = new IntegerAggregator(gfield, gfield == Aggregator.NO_GROUPING ? null : td.getFieldType(gfield), afield, aop);
        } else {
            aggregator = new StringAggregator(gfield, gfield == Aggregator.NO_GROUPING ? null : td.getFieldType(gfield), afield, aop);
        }
    }

    /**
     * @return If this aggregate is accompanied by a groupby, return the groupby
     * field index in the <b>INPUT</b> tuples. If not, return
     * {@link Aggregator#NO_GROUPING}
     */
    public int groupField() {
        // some code goes here
        return gfield;
    }

    /**
     * @return If this aggregate is accompanied by a group by, return the name
     * of the groupby field in the <b>OUTPUT</b> tuples. If not, return
     * null;
     */
    public String groupFieldName() {
        // some code goes here
        if (gfield < 0)
            return null;
        return tupleDesc.getFieldName(0);
    }

    /**
     * @return the aggregate field
     */
    public int aggregateField() {
        // some code goes here
        return afield;
    }

    /**
     * @return return the name of the aggregate field in the <b>OUTPUT</b>
     * tuples
     */
    public String aggregateFieldName() {
        // some code goes here
        if (gfield < 0)
            return tupleDesc.getFieldName(1);
        return tupleDesc.getFieldName(0);
    }

    /**
     * @return return the aggregate operator
     */
    public Aggregator.Op aggregateOp() {
        // some code goes here
        return aop;
    }

    public static String nameOfAggregatorOp(Aggregator.Op aop) {
        return aop.toString();
    }

    public void open() throws NoSuchElementException, DbException,
            TransactionAbortedException {
        // some code goes here
        child.open();
        super.open();

        while (child.hasNext()) {
            aggregator.mergeTupleIntoGroup(child.next());
        }
        aggIterator = aggregator.iterator();
        aggIterator.open();
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
        while (aggIterator.hasNext()) {
            Tuple t = aggIterator.next();
            return t;
        }
        return null;
    }

    public void rewind() throws DbException, TransactionAbortedException {
        // some code goes here
        child.rewind();
        aggIterator.rewind();
    }

    /**
     * Returns the TupleDesc of this Aggregate. If there is no group by field,
     * this will have one field - the aggregate column. If there is a group by
     * field, the first field will be the group by field, and the second will be
     * the aggregate value column.
     * <p>
     * The name of an aggregate column should be informative. For example:
     * "aggName(aop) (child_td.getFieldName(afield))" where aop and afield are
     * given in the constructor, and child_td is the TupleDesc of the child
     * iterator.
     */
    public TupleDesc getTupleDesc() {
        // some code goes here
        return tupleDesc;
    }

    public void close() {
        // some code goes here
        super.close();
        child.close();
    }

    @Override
    public OpIterator[] getChildren() {
        // some code goes here
        return new OpIterator[]{this.child};
    }

    @Override
    public void setChildren(OpIterator[] children) {
        // some code goes here
        if (this.child != children[0]) {
            this.child = children[0];
        }
    }

}
