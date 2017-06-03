package kr.kookmin.cs.bigdata.kkp;

import java.io.IOException;
import java.util.ArrayList;

import kr.kookmin.cs.bigdata.kkp.vec.Word2VEC;



public class word2vecTest {

	/**
	 * @param args
	 */
	public static void main(String[] args) throws Exception {
		
//		Preprocessing preprocessing = new Preprocessing();
		// TODO Auto-generated method stub
//		String FILE = "data/frWiki_no_lem_no_postag_no_phrase_1000_skip_cut100.bin";
//
//		System.out.println("Start....");
//		Word2VEC w1 = new Word2VEC();
//		w1.loadGoogleModel(FILE);
//
//		System.out.println(w1.distance("lover"));
//		System.out.println(w1.wordSimilarity("loves", "happy"));
		String testStr = "Who is loves";
		
		ArrayList<String> li = new ArrayList<String>();
		
		li = Preprocessing.removeNeedlessWords(testStr);
		
		System.out.println(li.get(0));
	}
		
		
	
	
	
}
