package kr.ac.kookmin.cs.bigdata.kkp;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Arrays;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.json.*;   // * 를 활용한 모든 라이브러리 포함은 위험

import com.google.gson.Gson;

import kr.ac.kookmin.cs.bigdata.kkp.vec.Word2VEC;

/*
 * This code is only Test code & Temp code
 */

public class word2vecTemp extends Configured implements Tool {
	
//	private static Word2VEC word2vec = new Word2VEC();
	private static String FILEPATH;
	public static void main(String[] args) throws Exception {
		FILEPATH = args[2];
//		word2vec.loadGoogleModel(FILEPATH);
		
//		System.out.println(w1.distance("lover"));
//		System.out.println(w1.wordSimilarity("loves", "happy"));
//		String testStr = "Who is likes";
//		
//		ArrayList<String> li = new ArrayList<String>();
//		
//		li = Preprocessing.removeNeedlessWords(testStr);
//		
//		System.out.println(li.get(0));
//		Gson gson = new Gson();
//        String testSerialization1 = gson.toJson(word2vec);
//		System.out.println(Long.parseLong("the"));
		
		
		System.out.println(Arrays.toString(args));
		Configuration config = new Configuration();
//		config.set("instance1", testSerialization1);
		config.set("filePath", args[2]);
//	    config.set("fs.default.name", "hdfs://" + "master" + ":9000") ;

		int res = ToolRunner.run(config, new word2vecTemp(), args);
		System.exit(res);
	}

	
	@Override
	public int run(String[] args) throws Exception {
		System.out.println(Arrays.toString(args));

		Job job = Job.getInstance(getConf());
		job.setJarByClass(word2vecTemp.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		job.setMapperClass(wordMap.class);
		job.setReducerClass(wordReduce.class);

		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);

		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		job.waitForCompletion(true);

		return 0;
	}

	public static class wordMap extends Mapper<LongWritable, Text, Text, Text> {
		Configuration conf;
		String filePath;
		Word2VEC word2vec = new Word2VEC();

		public void setup(Context context) {
			conf = context.getConfiguration();
			filePath = conf.get("filePath");
//			Path pt=new Path(filePath);//Location of file in HDFS
//	        FileSystem fs;
//			try {
//				fs = FileSystem.get(new Configuration());
//				BufferedReader br=new BufferedReader(new InputStreamReader(fs.open(pt)));
//		        String line;
//		        line=br.readLine();
//		        while (line != null){
//		            System.out.println(line);
//		            line=br.readLine();
//		        }
//			} catch (IOException e) {
//				// TODO Auto-generated catch block
//				e.printStackTrace();
//			}
	        
			
			
			try {
				word2vec.loadGoogleModel(filePath);
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}

		}

		@Override
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			String book;
			String description;
			String line = value.toString();
	
			context.write(new Text(line), new Text(word2vec.distance(line).toString()));
		}
	}

	public static class wordReduce extends
			Reducer<Text, Text, Text, Text> {

		@Override
		public void reduce(Text key, Iterable<Text> values,
				Context context) throws IOException, InterruptedException {
			for (Text val : values) {  // Calculating Average : Sum/Count
				context.write(key, val);
			}	
		}
	}	
}
