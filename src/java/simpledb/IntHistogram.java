package simpledb;

/** A class to represent a fixed-width histogram over a single integer-based field.
 */
public class IntHistogram {

    private int[] buckets;
    private double min;
    private double max;
    private int cnt;

    /**
     * Create a new IntHistogram.
     * 
     * This IntHistogram should maintain a histogram of integer values that it receives.
     * It should split the histogram into "buckets" buckets.
     * 
     * The values that are being histogrammed will be provided one-at-a-time through the "addValue()" function.
     * 
     * Your implementation should use space and have execution time that are both
     * constant with respect to the number of values being histogrammed.  For example, you shouldn't 
     * simply store every value that you see in a sorted list.
     * 
     * @param buckets The number of buckets to split the input value into.
     * @param min The minimum integer value that will ever be passed to this class for histogramming
     * @param max The maximum integer value that will ever be passed to this class for histogramming
     */
    public IntHistogram(int buckets, int min, int max) {
    	// some code goes here
        this.buckets = new int[buckets];
        this.min = min;
        this.max = max;
        this.cnt = 0;
    }

    /**
     * Add a value to the set of values that you are keeping a histogram of.
     * @param v Value to add to the histogram
     */
    public void addValue(int v) {
    	// some code goes here
        int N = buckets.length;
        int index = (int) ((v - min) * N / (max - min+1));
        buckets[index]++;
        cnt++;
    }

    /**
     * Estimate the selectivity of a particular predicate and operand on this table.
     * 
     * For example, if "op" is "GREATER_THAN" and "v" is 5, 
     * return your estimate of the fraction of elements that are greater than 5.
     * 
     * @param op Operator
     * @param v Value
     * @return Predicted selectivity of this particular operator and value
     */
    public double estimateSelectivity(Predicate.Op op, int v) {

    	// some code goes here
        double res = 0;
        int N = buckets.length;
        if (v < min || v > max) {
            return outBoundEstimate(op,v < min);
        }
        int index = (int) ((v - min) * N / (max - min+1));
        double left = (max-min+1)*index/N + min-1;
        double right = (max-min+1)*(index+1)/N + min-1;
        double width = right -left < 1? 1: right-left;
        switch (op){
            case EQUALS:
                res = buckets[index]/(width*cnt);
                break;
            case LESS_THAN_OR_EQ:
                // 区间是(]左开右闭 会造成一点误差
                res = getLess(index)/cnt + (v-left+1)*buckets[index]/(width*cnt);
                break;
            case LESS_THAN:
                res = getLess(index)/cnt + (v-left)*buckets[index]/(width*cnt);
                break;
            case GREATER_THAN:
                res = getLarger(index)/cnt + (right-v)*buckets[index]/(width*cnt);
                break;
            case GREATER_THAN_OR_EQ:
                res = getLarger(index)/cnt + (right-v+1)*buckets[index]/(width*cnt);
                break;
            case NOT_EQUALS:
                res = 1 - buckets[index]/(width*cnt);
        }

        return res;
    }

    private double outBoundEstimate(Predicate.Op op,boolean outLeft) {
        double res = 0;
        switch (op){
            case EQUALS:
                res = 0;
                break;
            case LESS_THAN_OR_EQ:
            case LESS_THAN:
                res = outLeft?0:1;
                break;
            case GREATER_THAN:
            case GREATER_THAN_OR_EQ:
                res = outLeft ? 1:0;
                break;
            case NOT_EQUALS:
                res = 1;
        }
        return res;
    }

    private double getLess(int index) {
        double res = 0;
        for (int i=0;i<index;i++) {
            res = res + buckets[i];
        }
        return res;
    }
    private double getLarger(int index) {
        double res = 0;
        for (int i=index+1;i<buckets.length;i++) {
            res = res + buckets[i];
        }
        return res;
    }
    
    /**
     * @return
     *     the average selectivity of this histogram.
     *     
     *     This is not an indispensable method to implement the basic
     *     join optimization. It may be needed if you want to
     *     implement a more efficient optimization
     * */
    public double avgSelectivity()
    {
        // some code goes here
        return 1.0;
    }
    
    /**
     * @return A string describing this histogram, for debugging purposes
     */
    public String toString() {
        // some code goes here
        String res = "";
        int len = (int) ((max-min)/buckets.length);
        for (int i=0;i<buckets.length;i++) {
            res += "left: "+i*len;
            res += "\tright: " + (i+1)*len;
            res += "\theight(cnt): " + buckets[i];
            res += "\n";
        }
        return res;
    }
}
