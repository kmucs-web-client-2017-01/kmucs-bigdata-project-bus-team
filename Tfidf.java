package kr.ac.kookmin.cs.bigdata;

import java.util.Arrays;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class Tfidf extends Configured implements Tool {

	static String SERVER_PATH = "/student2/KiHyeonPark/" ;
	static String OUTPUTPATH5 = SERVER_PATH + "output5" ;
	static String INPUTPATH4 = SERVER_PATH + "output3/part-r-00000" ;
	static String OUTPUTPATH4 = SERVER_PATH + "output4" ;
	static String INPUTPATH3 = SERVER_PATH + "output2/part-r-00000" ;
	static String OUTPUTPATH3 = SERVER_PATH + "output3" ;
	static String INPUTPATH2 = SERVER_PATH + "output/part-r-00000" ;
	static String OUTPUTPATH2 = SERVER_PATH + "output2" ;
	
//	static String OUTPUTPATH5 = "/home/kmucs/workspace/WordCount/output5" ;
//	static String INPUTPATH4 = "/home/kmucs/workspace/WordCount/output3/part-r-00000" ;
//	static String OUTPUTPATH4 = "/home/kmucs/workspace/WordCount/output4" ;
//	static String INPUTPATH3 = "/home/kmucs/workspace/WordCount/output2/part-r-00000" ;
//	static String OUTPUTPATH3 = "/home/kmucs/workspace/WordCount/output3" ;
//	static String INPUTPATH2 = "/home/kmucs/workspace/WordCount/output/part-r-00000" ;
//	static String OUTPUTPATH2 = "/home/kmucs/workspace/WordCount/output2" ;
	static String INPUTPATH ;
	static String OUTPUTPATH ;
	static int numofAsin = 0 ;
	
	public static void main(String[] args) throws Exception {
		System.out.println(Arrays.toString(args));
		int res = ToolRunner.run(new Configuration(), new Tfidf(), args);

		System.exit(res);
	}

	@Override
	public int run(String[] args) throws Exception {
		INPUTPATH = args[0] ;
		OUTPUTPATH = args[1];
		ToolRunner.run(new Configuration(), new WordCount(), args);
		ToolRunner.run(new Configuration(), new AsinCount(), args);
		ToolRunner.run(new Configuration(), new WordCountForAsin(), args);
		ToolRunner.run(new Configuration(), new CalculateTFIDF(), args);
		ToolRunner.run(new Configuration(), new TopwordInTFIDF(), args);
		return 1 ;
	}

	public static String StringReplace(String str) {
		String match = "[^0-9a-zA-Z0-9\\s]";
		str = str.replaceAll(match, "");
		return str;
	}

}
