package kr.kookmin.cs.bigdata.kkp;

import java.util.HashSet;
import java.io.BufferedReader; 
import java.io.File;
import java.io.IOException; 
import java.io.StringReader; 
import me.xiaosheng.word2vec.*;
import me.xiaosheng.util.*;
import org.apache.lucene.analysis.PorterStemFilter;
import org.tartarus.martin.Stemmer;
import com.medallia.word2vec.Searcher.Match;
import com.medallia.word2vec.Searcher.UnknownWordException;
import com.medallia.word2vec.Word2VecTrainerBuilder.TrainingProgressListener;
import com.medallia.word2vec.neuralnetwork.NeuralNetworkType;
import com.medallia.word2vec.thrift.Word2VecModelThrift;
import com.medallia.word2vec.util.AutoLog;
import com.medallia.word2vec.util.Common;
import com.medallia.word2vec.util.Format;
import com.medallia.word2vec.util.ProfilingTimer;
import com.medallia.word2vec.util.Strings;
import com.medallia.word2vec.util.ThriftUtils;
import com.medallia.word2vec.*;
import com.ansj.vec.domain.WordEntry;
import me.xiaosheng.util.Segment;


public class word2vecTest {

	/**
	 * @param args
	 */
	public static void main(String[] args) throws Exception  {
		// TODO Auto-generated method stub
		String FILE_PATH = "/home/foscar/workspace/Bus/GoogleNews-vectors-negative300.bin";
		System.out.println("Start..");
		try {
			Word2Vec vec = new Word2Vec();
			vec.loadGoogleModel(FILE_PATH);
//			System.out.println("beijing | china" + vec.wordSimilarity("beijing", "china"));
//			System.out.println("Finished..");
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		
		
		
	}
	
	
	
}
