package simpledb.execution.vectorize;

import simpledb.common.DbException;
import simpledb.execution.OpIterator;
import simpledb.execution.Operator;
import simpledb.execution.Predicate;
import simpledb.storage.Tuple;
import simpledb.storage.TupleDesc;
import simpledb.transaction.TransactionAbortedException;

import java.util.NoSuchElementException;

public class FilterVec extends OperatorVec {
    private static final long serialVersionUID = 1L;

    private Predicate predicate; // The predicate to filter tuples with
    private OpIteratorVec child; // The child operator
    private TupleDesc tupleDesc;

    /**
     * Constructor accepts a predicate to apply and a child operator to read
     * tuples to filter from.
     *
     * @param p
     *            The predicate to filter tuples with
     * @param child
     *            The child operator
     */
    public FilterVec(Predicate p, OpIteratorVec child) {
        // some code goes here
        this.predicate = p;
        this.child = child;
        this.tupleDesc = child.getTupleDesc();
    }

    @Override
    public void open() throws DbException, TransactionAbortedException {
        child.open();
        super.open();
    }

    @Override
    public OpIteratorVec[] getChildren() {
        return new OpIteratorVec[] {this.child};
    }

    @Override
    public void setChildren(OpIteratorVec[] children) {

    }

    @Override
    protected Chunk fetchNext() throws DbException, TransactionAbortedException {
        if (!child.hasNext()) return null;
        Chunk result = new Chunk(this.tupleDesc);
        while (child.hasNext()) {
            if (result.isFull()) {
                return result;
            }
            Chunk chunk = child.next();
            if (chunk == null || chunk.getTuples().size() == 0) {
                return null;
            }
            for (Tuple tuple : chunk.getTuples()) {
                if (predicate.filter(tuple)) {
                    result.add(tuple);
                }
            }
//            Tuple t = child.next();
//            if (predicate.filter(t)) {
//                result.add(t);
//            }
        }
        if (result.getSize() == 0)
            return null;

        return result;
    }

    @Override
    public void rewind() throws DbException, TransactionAbortedException {
        this.child.rewind();
    }

    @Override
    public TupleDesc getTupleDesc() {
        return tupleDesc;
    }

    @Override
    public void close() {
        this.child.close();
    }
}
