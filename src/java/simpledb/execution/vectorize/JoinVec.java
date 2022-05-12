package simpledb.execution.vectorize;

import simpledb.common.DbException;
import simpledb.execution.JoinPredicate;
import simpledb.execution.OpIterator;
import simpledb.execution.Predicate;
import simpledb.storage.*;
import simpledb.transaction.TransactionAbortedException;

import java.util.*;

public class JoinVec extends OperatorVec {
    private static final long serialVersionUID = 1L;
    /* The predicate to use to join the children */
    private JoinPredicate predicate;
    /* Iterator for the left(outer) relation to join */
    private OpIteratorVec child1;
    /* Iterator for the right(inner) relation to join */
    private OpIteratorVec child2;
    private TupleDesc tupleDesc;
    private Tuple t1;
    private Tuple t2;
    private int t1Index ;
    private int t2Index ;
    private Map<String, List<Tuple>> map;

    private Chunk chunk1;
    private Chunk chunk2;


    public JoinVec(JoinPredicate p, OpIteratorVec child1, OpIteratorVec child2) {
        // some code goes here
        this.predicate = p;
        this.child1 = child1;
        this.child2 = child2;

        this.tupleDesc = TupleDesc.merge(child1.getTupleDesc(), child2.getTupleDesc());
    }

    public JoinPredicate getJoinPredicate() {
        // some code goes here
        return predicate;
    }

    public String getJoinField1Name() {
        // some code goes here
        int field1 = predicate.getField1();
        return child1.getTupleDesc().getFieldName(field1);
    }

    /**
     * @return
     *       the field name of join field2. Should be quantified by
     *       alias or table name.
     * */
    public String getJoinField2Name() {
        // some code goes here
        int field2 = predicate.getField2();
        return child2.getTupleDesc().getFieldName(field2);
    }

    @Override
    public void rewind() throws DbException, TransactionAbortedException {
        child1.rewind();
        child2.rewind();
    }

    @Override
    protected Chunk fetchNext() throws DbException, TransactionAbortedException {

        while (chunk1 != null || child1.hasNext()) {
            if (chunk1 == null) {
                chunk1 = child1.next();
                t1Index = 0;
            }

            for (int i = t1Index; i < chunk1.getSize(); i++) {
                t1 = chunk1.getTuples().get(i);
                while (child2.hasNext() ) {
                    Chunk chunk2 = child2.next();
                    boolean[] res = predicate.filterVec(t1, chunk2);
                    Chunk result = new Chunk(tupleDesc);
                    for (int k = 0; k < res.length; k ++) {
                        if (res[k]) {
                            t2 = chunk2.getTuples().get(k);
                            Tuple tuple = new Tuple(tupleDesc);
                            Iterator<Field> it1 = t1.fields();
                            Iterator<Field> it2 = t2.fields();
                            int j = 0;
                            while (it1.hasNext()) {
                                tuple.setField(j ++, it1.next());
                            }
                            while (it2.hasNext()) {
                                tuple.setField(j ++, it2.next());
                            }
                            result.add(tuple);
                        }
                    }
                    if (result.getSize() != 0) {
                        return result;
                    }
                }
                child2.rewind();
                t1Index = i + 1;
            }
            chunk1 = null;
        }

        return null;
    }

    @Override
    public OpIteratorVec[] getChildren() {
        return new OpIteratorVec[] {this.child1, this.child2};
    }

    @Override
    public void setChildren(OpIteratorVec[] children) {
        if (this.child1 != children[0]) {
            this.child1 = children[0];
        }
        if (children.length > 1 && this.child2 != children[1]) {
            this.child2 = children[1];
        }
    }

    @Override
    public TupleDesc getTupleDesc() {
        return tupleDesc;

    }

    public void open() throws DbException, NoSuchElementException,
            TransactionAbortedException {
        // some code goes here
        child1.open();
        child2.open();

        super.open();
    }

    public void close() {
        // some code goes here
        child1.close();
        child2.close();
        super.close();
    }

    private Map<String, List<Tuple>> buildHashTableForChild1() throws TransactionAbortedException, DbException {
        Map<String, List<Tuple>> hashTable = new HashMap<>();
        while (child1.hasNext()) {
            Chunk chunk = child1.next();
            for (Tuple t : chunk.getTuples()) {
                int field1 = predicate.getField1();
                Field field = t.getField(field1);
                String key = "";
                if (field instanceof IntField) {
                    IntField intField = (IntField)field;
                    key = String.valueOf(intField.getValue());
                } else {
                    StringField stringField = (StringField) field;
                    key = stringField.getValue();
                }
                if (!hashTable.containsKey(key)) {
                    hashTable.put(key, new ArrayList<>());
                }
                hashTable.get(key).add(t);
            }
        }
        return hashTable;
    }

    private void buildHashTableForChild2() throws TransactionAbortedException, DbException {
        Map<String, List<Tuple>> hashTable = new HashMap<>();

        while (child2.hasNext()) {
            Chunk chunk = child2.next();
            for (Tuple t : chunk.getTuples()) {
                int field1 = predicate.getField1();
                Field field = t.getField(field1);
                String key = "";
                if (field instanceof IntField) {
                    IntField intField = (IntField)field;
                    key = String.valueOf(intField.getValue());
                } else {
                    StringField stringField = (StringField) field;
                    key = stringField.getValue();
                }
                if (!hashTable.containsKey(key)) {
                    hashTable.put(key, new ArrayList<>());
                }
                hashTable.get(key).add(t);
            }

        }
    }

}
