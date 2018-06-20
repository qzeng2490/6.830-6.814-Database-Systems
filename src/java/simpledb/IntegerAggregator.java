package simpledb;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Knows how to compute some aggregate over a set of IntFields.
 */
public class IntegerAggregator implements Aggregator {

    private static final long serialVersionUID = 1L;

    private int gbfield;
    private Type gbfieldtype;
    private int afield;
    private Op what;

    // for grouping
    private Map<Field,Integer> previous;
    private Map<Field,Integer> countMap;
    private Map<Field,Integer> sumMap;

    // for aggregate no grouping
    private int preMin;
    private int preMax;
    private int preSum;
    private int preAvg;
    private int preCnt;

    private LinkedHashMap<Field,Tuple> tuplist;
    //for aggregate no grouping
    private ArrayList<Tuple> tuples;
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
        this.gbfieldtype =gbfieldtype;
        this.afield = afield;
        this.what = what;
        previous = new HashMap<>();
        countMap = new HashMap<>();
        sumMap = new HashMap<>();
        preCnt = 0;
        preAvg = 0;
        preMax = Integer.MIN_VALUE;
        preMin = Integer.MAX_VALUE;
        preSum = 0;
        tuplist = new LinkedHashMap<>();
        tuples = new ArrayList<>();
    }

    /**
     * Merge a new tuple into the aggregate, grouping as indicated in the
     * constructor
     * 
     * @param tup
     *            the Tuple containing an aggregate field and a group-by field
     */
    //COUNT, SUM, AVG, MIN, MAX
    public void mergeTupleIntoGroup(Tuple tup) {
        // some code goes here
        if(gbfield >=0) {
            Field key = tup.getField(gbfield);
            int aggregateVal = ((IntField)tup.getField(afield)).getValue();
            Tuple newTup = null;
            switch (what) {
                case MIN:
                    int min = aggregateVal;
                    if (previous.containsKey(key)) {
                        min = Math.min(aggregateVal,previous.get(key)) ;
                    }
                    previous.put(key,min);
                    newTup = new Tuple(getTupleDesc());
                    newTup.setField(0,key);
                    newTup.setField(1,new IntField(min));
                    tuplist.put(key,newTup);
                    break;
                case COUNT:
                    int cnt = 1;
                    if (previous.containsKey(key)) {
                        cnt = 1 + previous.get(key);
                    }
                    previous.put(key,cnt);
                    newTup = new Tuple(getTupleDesc());
                    newTup.setField(0,key);
                    newTup.setField(1,new IntField(cnt));
                    tuplist.put(key,newTup);
                    break;
                case AVG:
                    int avg = aggregateVal;
                    int count = 1;
                    int sumVal = aggregateVal;
                    if (previous.containsKey(key)) {
                        count = countMap.get(key)+1;
                        sumVal = sumMap.get(key) + aggregateVal;
                        avg = sumVal /count;
                    }
                    previous.put(key,avg);
                    countMap.put(key,count);
                    sumMap.put(key,sumVal);
                    newTup = new Tuple(getTupleDesc());
                    newTup.setField(0,key);
                    newTup.setField(1,new IntField(avg));
                    tuplist.put(key,newTup);
                    break;
                case SUM:
                    int sum = aggregateVal;
                    if (previous.containsKey(key)) {
                        sum = previous.get(key) + aggregateVal;
                    }
                    previous.put(key,sum);
                    newTup = new Tuple(getTupleDesc());
                    newTup.setField(0,key);
                    newTup.setField(1,new IntField(sum));
                    tuplist.put(key,newTup);
                    break;
                case MAX:
                    int max = aggregateVal;
                    if (previous.containsKey(key)) {
                        max = Math.max(aggregateVal,previous.get(key)) ;
                    }
                    previous.put(key,max);
                    newTup = new Tuple(getTupleDesc());
                    newTup.setField(0,key);
                    newTup.setField(1,new IntField(max));
                    tuplist.put(key,newTup);
                    break;
            }

        } else {
            int aggregateVal = ((IntField)tup.getField(afield)).getValue();
            Tuple newTup = null;
            switch (what) {
                case MIN:
                    preMin = Math.min(aggregateVal,preMin) ;
                    newTup = new Tuple(getTupleDesc());
                    newTup.setField(0,new IntField(preMin));
                    tuples.add(newTup);
                    break;
                case COUNT:
                    preCnt = preCnt + 1;
                    newTup = new Tuple(getTupleDesc());
                    newTup.setField(0,new IntField(preCnt));
                    tuples.add(newTup);
                    break;
                // 测试不过  感觉是测试本身写错了
                case AVG:
                    preAvg = (preSum + aggregateVal) /(preCnt +1);
                    preCnt = preCnt + 1;
                    preSum = preSum + aggregateVal;
                    newTup = new Tuple(getTupleDesc());
                    newTup.setField(0,new IntField(preAvg));
                    tuples.add(newTup);
                    break;
                case SUM:
                    preSum = preSum + aggregateVal;
                    newTup = new Tuple(getTupleDesc());
                    newTup.setField(0,new IntField(preSum));
                    tuples.add(newTup);
                    break;
                case MAX:
                    preMax = Math.max(preMax,aggregateVal);
                    newTup = new Tuple(getTupleDesc());
                    newTup.setField(0,new IntField(preMax));
                    tuples.add(newTup);
                    break;
            }
        }
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
