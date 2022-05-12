package simpledb.execution.vectorize;

import simpledb.common.Database;
import simpledb.common.DbException;
import simpledb.common.Type;
import simpledb.execution.OpIterator;
import simpledb.storage.DbFile;
import simpledb.storage.DbFileIterator;
import simpledb.storage.Tuple;
import simpledb.storage.TupleDesc;
import simpledb.transaction.TransactionAbortedException;
import simpledb.transaction.TransactionId;

import java.util.NoSuchElementException;

public class SeqScanVec implements OpIteratorVec {

    private static final long serialVersionUID = 1L;

    private TransactionId transactionId;
    private int tableId;
    private String tableAlias;

    private DbFile dbFile;
    private DbFileIterator iter;

    private TupleDesc desc;

    public SeqScanVec(TransactionId tid, int tableid, String tableAlias) {
        // some code goes here
        this.transactionId = tid;
        this.tableAlias = tableAlias;
        this.tableId = tableid;
        this.dbFile = Database.getCatalog().getDatabaseFile(tableid);
        iter = dbFile.iterator(transactionId);
        this.desc = getTupleDesc();
    }

    @Override
    public void open() throws DbException, TransactionAbortedException {
        iter.open();
    }

    @Override
    public boolean hasNext() throws DbException, TransactionAbortedException {
        return iter.hasNext();
    }

    @Override
    public Chunk next() throws DbException, TransactionAbortedException, NoSuchElementException {
        if (!iter.hasNext()) {
            return null;
        }
        Chunk result = new Chunk(this.desc);
        while (iter.hasNext()) {
            if (result.isFull()) {
                return result;
            }
            Tuple t = iter.next();
            result.add(t);
        }
        return result;
    }

    @Override
    public void rewind() throws DbException, TransactionAbortedException {
        iter.rewind();
    }

    @Override
    public TupleDesc getTupleDesc() {
        TupleDesc td = Database.getCatalog().getTupleDesc(dbFile.getId());
        Type[] types = new Type[td.numFields()];
        String[] names = new String[td.numFields()];

        for (int i=0; i<td.numFields(); i++) {
            types[i] = td.getFieldType(i);
            names[i] = tableAlias+'.'+td.getFieldName(i);
        }
        return new TupleDesc(types, names);
    }

    @Override
    public void close() {
        iter.close();
    }
}
