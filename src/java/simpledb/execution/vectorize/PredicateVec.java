package simpledb.execution.vectorize;

import simpledb.execution.Predicate;
import simpledb.storage.Field;
import simpledb.storage.Tuple;

import java.io.Serializable;

/**
 * Predicate compares tuples to a specified Field value.
 */
public class PredicateVec extends Predicate {

    private static final long serialVersionUID = 1L;


    /**
     * Constructor.
     *
     * @param field
     *            field number of passed in tuples to compare against.
     * @param op
     *            operation to use for comparison
     * @param operand
     *            field value to compare passed in tuples to
     */
    public PredicateVec(int field, Op op, Field operand) {
        super(field, op, operand);
    }



    /**
     * Compares the field number of t specified in the constructor to the
     * operand field specified in the constructor using the operator specific in
     * the constructor. The comparison can be made through Field's compare
     * method.
     * 
     * @param chunk
     *            The tuple to compare against
     * @return true if the comparison is true, false otherwise.
     */
    public boolean[] filterVec(Chunk chunk) {
        // some code goes here
        boolean[] result = new boolean[chunk.getSize()];
        int i = 0;
        for (Tuple t : chunk.getTuples()) {
            Field field = t.getField(getField());
            if (field.compare(getOp(), getOperand())) {
                result[i ++] = true;
            }
        }

        return result;
    }

    /**
     * Returns something useful, like "f = field_id op = op_string operand =
     * operand_string"
     */
    public String toString() {
        // some code goes here
        StringBuffer sb = new StringBuffer();
        sb.append("f = ");
        sb.append(getField());
        sb.append(" op = ");
        sb.append(getOp().toString());
        sb.append("operand = ");
        sb.append(getOperand().toString());
        return sb.toString();
    }
}
