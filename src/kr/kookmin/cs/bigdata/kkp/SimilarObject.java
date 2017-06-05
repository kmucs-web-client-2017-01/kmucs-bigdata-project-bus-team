package kr.kookmin.cs.bigdata.kkp;

/*
 *  Object For Sorting asin*similarity
 */

public class SimilarObject implements Comparable<SimilarObject> {
	private String asin;
	private float similarity;
	
	public SimilarObject(String asin, float sim) {
		this.asin = asin; similarity = sim;
	}
	
	public void set(String asin, float sim) {
		this.asin = asin; similarity = sim;
	}
	
	@Override
    public int compareTo(SimilarObject target) {
        if (this.similarity > target.similarity) {
            return 1;
        } else if (this.similarity < target.similarity) {
            return -1;
        }
        return 0;
    }
	
	@Override
    public String toString() {
		return asin + " : " + similarity;  
    }
	
}
