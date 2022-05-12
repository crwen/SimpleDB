package simpledb.execution.vectorize;

import simpledb.storage.Tuple;
import simpledb.storage.TupleDesc;

import java.util.ArrayList;
import java.util.List;

public class Chunk {
    private int LIMIT = 4096;
    private int capacity;
    private int size = 0;
    private int tuplePerSize;
    private TupleDesc desc;
    private List<Tuple>tuples;


    public Chunk(TupleDesc desc) {
        this.desc = desc;
        this.tuplePerSize = desc.getSize();
        capacity = LIMIT / tuplePerSize;
        tuples = new ArrayList<>(capacity);
    }

    public boolean isFull() {
        return size >= capacity;
    }

    public boolean add(Tuple t) {
        if (tuples.size() >= capacity) {
            return false;
        }
        tuples.add(t);
        size ++;
        return true;
    }

    public List<Tuple> getTuples() {
        return tuples;
    }

    public int getCapacity() {
        return this.capacity;
    }

    public int getSize() {
        return this.size;
    }
}
