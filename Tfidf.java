package kr.ac.kookmin.cs.bigdata;

import java.util.Arrays;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class Tfidf extends Configured implements Tool {

	static String INPUTPATH2 = "/home/kmucs/workspace/WordCount/output/part-r-00000" ;
	static String OUTPUTPATH2 = "/home/kmucs/workspace/WordCount/output2" ;
	static String INPUTPATH ;
	static String OUTPUTPATH ;
	
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
		ToolRunner.run(new Configuration(), new WordCountForAsin(), args);
		return 1 ;
	}

	public static String StringReplace(String str) {
		String match = "[^0-9a-zA-Z0-9\\s]";
		str = str.replaceAll(match, "");
		return str;
	}

}
