package simpledb;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Knows how to compute some aggregate over a set of StringFields.
 */
public class StringAggregator implements Aggregator {

    private static final long serialVersionUID = 1L;

    private int gbfield;
    private Type gbfieldtype;
    private int afield;
    private Op what;

    private Map<Field,Integer> countMap;

    private int preCnt;

    private LinkedHashMap<Field,Tuple> tuplist;
    private ArrayList<Tuple> tuples;
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
        if (what != Op.COUNT) throw new IllegalArgumentException("what != COUNT");
        this.gbfield = gbfield;
        this.gbfieldtype = gbfieldtype;
        this.afield = afield;
        this.what = what;
        countMap = new HashMap<>();
        preCnt = 0;
        tuplist = new LinkedHashMap<>();
        tuples = new ArrayList<>();
    }

    /**
     * Merge a new tuple into the aggregate, grouping as indicated in the constructor
     * @param tup the Tuple containing an aggregate field and a group-by field
     */
    public void mergeTupleIntoGroup(Tuple tup) {
        // some code goes here
        if(gbfield >=0) {
            Field key = tup.getField(gbfield);
            int cnt = 1;
            if (countMap.containsKey(key)) {
                cnt = 1 + countMap.get(key);
            }
            countMap.put(key,cnt);
            Tuple newTup = new Tuple(getTupleDesc());
            newTup.setField(0,key);
            newTup.setField(1,new IntField(cnt));
            tuplist.put(key,newTup);

        }else {
            preCnt = preCnt + 1;
            Tuple newTup = new Tuple(getTupleDesc());
            newTup.setField(0,new IntField(preCnt));
            tuples.add(newTup);
        }
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
        TupleDesc td = getTupleDesc();
        if (gbfield >= 0) {
            return new TupleIterator(td, tuplist.values());
        }else {
            return new TupleIterator(td, tuples);
        }
    }

    private TupleDesc getTupleDesc() {
        int N = gbfield >=0 ? 2 :1;
        Type[] typeAr = new Type[N];
        if (gbfield >= 0) {
            // groupVal
            typeAr[0] = gbfieldtype;
            // aggregateVal
            typeAr[1] = Type.INT_TYPE;
        } else {
            typeAr[0] = Type.INT_TYPE;
        }
        return new TupleDesc(typeAr);
    }

}
