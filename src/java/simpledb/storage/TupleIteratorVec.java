package simpledb.storage;

import simpledb.execution.OpIterator;
import simpledb.execution.vectorize.Chunk;
import simpledb.execution.vectorize.OpIteratorVec;

import java.util.Iterator;

/**
 * Implements a OpIterator by wrapping an Iterable<Tuple>.
 */
public class TupleIteratorVec implements OpIteratorVec {
    /**
	 *
	 */
    private static final long serialVersionUID = 1L;
    Iterator<Tuple> i = null;
    TupleDesc td = null;
    Iterable<Tuple> tuples = null;

    /**
     * Constructs an iterator from the specified Iterable, and the specified
     * descriptor.
     *
     * @param tuples
     *            The set of tuples to iterate over
     */
    public TupleIteratorVec(TupleDesc td, Iterable<Tuple> tuples) {
        this.td = td;
        this.tuples = tuples;

        // check that all tuples are the right TupleDesc
        for (Tuple t : tuples) {
            if (!t.getTupleDesc().equals(td))
                throw new IllegalArgumentException(
                        "incompatible tuple in tuple set");
        }
    }

    public void open() {
        i = tuples.iterator();
    }

    public boolean hasNext() {
        return i.hasNext();
    }

    public Chunk next() {
        if (!i.hasNext()) {
            return null;
        }
        Chunk chunk = new Chunk(td);
        while (i.hasNext()) {
            if (chunk.isFull()) {
                return chunk;
            }
            chunk.add(i.next());
        }
        return chunk;
    }

    public void rewind() {
        close();
        open();
    }

    public TupleDesc getTupleDesc() {
        return td;
    }

    public void close() {
        i = null;
    }
}
