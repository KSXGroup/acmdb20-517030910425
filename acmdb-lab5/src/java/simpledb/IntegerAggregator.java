package simpledb;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;

/**
 * Knows how to compute some aggregate over a set of IntFields.
 */
public class IntegerAggregator implements Aggregator {

    private static final long serialVersionUID = 1L;
    private int gbfield;
    private int afield;
    private Op what;
    private HashMap<Field, Integer> value;
    private HashMap<Field, Integer> count;
    private TupleDesc td;

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
        this.afield = afield;
        this.what = what;
        this.value = new HashMap<>();
        this.count = new HashMap<>();
        if(gbfield != NO_GROUPING)
            this.td = new TupleDesc(new Type[]{gbfieldtype, Type.INT_TYPE}, new String[]{"groupVal", "aggregateVal"});
        else
            this.td = new TupleDesc(new Type[]{Type.INT_TYPE}, new String[]{"aggregateVal"});
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
        Field curGbField = null;
        if(this.gbfield != NO_GROUPING)
            curGbField = tup.getField(this.gbfield);
        Integer oldValue = this.value.get(curGbField);
        Integer oldCount = this.count.get(curGbField);
        if(oldCount == null) oldCount = 0;
        this.count.put(curGbField, oldCount + 1);
        IntField curAField = (IntField)tup.getField(this.afield);
        int curAValue = curAField.getValue();
        if(oldValue == null)
            this.value.put(curGbField, curAValue);
        else {
            switch (this.what) {
                case COUNT:
                    this.value.put(curGbField, oldCount + 1);
                    break;
                case MIN:
                    if (oldValue > curAValue) this.value.put(curGbField, curAValue);
                    break;
                case AVG:
                case SUM:
                    this.value.put(curGbField, curAValue + oldValue);
                    break;
                case MAX:
                    if (oldValue < curAValue) this.value.put(curGbField, curAValue);
                    break;
                case SUM_COUNT:
                case SC_AVG:
                    break;
            }
        }
    }

    /**
     * Create a DbIterator over group aggregate results.
     * 
     * @return a DbIterator whose tuples are the pair (groupVal, aggregateVal)
     *         if using group, or a single (aggregateVal) if no grouping. The
     *         aggregateVal is determined by the type of aggregate specified in
     *         the constructor.
     */
    public DbIterator iterator() {
        // some code goes here
        Tuple tmp;
        Iterator<Field> fieldIterator = this.value.keySet().iterator();
        ArrayList<Tuple> tuples = new ArrayList<>();
        while(fieldIterator.hasNext()){
            tmp = new Tuple(this.td);
            Field cf = fieldIterator.next();
            if(this.gbfield != NO_GROUPING){
                tmp.setField(0, cf);
                if(this.what == Op.AVG){
                    tmp.setField(1, new IntField(this.value.get(cf) / this.count.get(cf)));
                    tuples.add(tmp);
                }
                else {
                    tmp.setField(1, new IntField(this.value.get(cf)));
                    tuples.add(tmp);
                }
            }
            else {
                if(this.what == Op.AVG){
                    tmp.setField(0, new IntField(this.value.get(cf) / this.count.get(cf)));
                    tuples.add(tmp);
                }
                else {
                    tmp.setField(0, new IntField(this.value.get(cf)));
                    tuples.add(tmp);
                }
            }
        }
        return new TupleIterator(this.td, tuples);
    }

}
