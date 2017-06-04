package kr.kookmin.cs.bigdata.kkp;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
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

import kr.kookmin.cs.bigdata.kkp.vec.Word2VEC;



public class word2vecTest extends Configured implements Tool {
	
	private static Word2VEC word2vec = new Word2VEC();
	private static String FILEPATH;
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
//		String testStr = "Who is likes";
//		
//		ArrayList<String> li = new ArrayList<String>();
//		
//		li = Preprocessing.removeNeedlessWords(testStr);
//		
//		System.out.println(li.get(0));
		
		System.out.println(Arrays.toString(args));
		
		int res = ToolRunner.run(new Configuration(), new word2vecTest(), args);
		System.exit(res);
	}

	
	@Override
	public int run(String[] args) throws Exception {
		System.out.println(Arrays.toString(args));
		
		FILEPATH = args[2];
		word2vec.loadGoogleModel(FILEPATH);
		
		
		Job job = Job.getInstance(getConf());
		job.setJarByClass(word2vecTest.class);
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

	public static class wordMap extends
			Mapper<LongWritable, Text, Text, Text> {

		@Override
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			String book;
			String description;
			String line = value.toString();
//			Word2VEC word2vec = new Word2VEC();
//			word2vec.loadGoogleModel(FILEPATH);

			try {
				JSONObject obj = new JSONObject(line); // JSON from 1 line
				if(obj.has("description")) {
					book = obj.getString("asin");
					description = obj.getString("description");
					String sample = Preprocessing.removeNeedlessWords(description).get(0);
//					Float f = word2vec.wordSimilarity(sample, "love");
					context.write(new Text(book), new Text( sample + " " + word2vec.distance(sample).toString()));
				}
			} catch (JSONException e) {
				e.printStackTrace();
			}
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
