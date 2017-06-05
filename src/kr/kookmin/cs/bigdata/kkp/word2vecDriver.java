package kr.kookmin.cs.bigdata.kkp;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.PriorityQueue;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.FloatWritable;
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

import kr.kookmin.cs.bigdata.kkp.vec.Word2VEC;



public class word2vecDriver extends Configured implements Tool {
		
	public static void main(String[] args) throws Exception {
		
		System.out.println(Arrays.toString(args));
		
		Configuration config = new Configuration();
		config.set("filePath", args[0]);

		int res = ToolRunner.run(config, new word2vecDriver(), args);
		System.exit(res);
	}

	
	@Override
	public int run(String[] args) throws Exception {
		System.out.println(Arrays.toString(args));

		Job step1Job = Job.getInstance(getConf());
		step1Job.setJarByClass(word2vecDriver.class);
		step1Job.setOutputKeyClass(Text.class);
		step1Job.setOutputValueClass(FloatWritable.class);

		step1Job.setMapperClass(step1Map.class);
		step1Job.setReducerClass(step1Reduce.class);

		step1Job.setInputFormatClass(TextInputFormat.class);
		step1Job.setOutputFormatClass(TextOutputFormat.class);

		FileInputFormat.addInputPath(step1Job, new Path(args[1]));
		FileOutputFormat.setOutputPath(step1Job, new Path(args[2]));

		step1Job.waitForCompletion(true);
		
		
		Job step2Job = Job.getInstance(getConf());
		step2Job.setJarByClass(word2vecDriver.class);
		step2Job.setOutputKeyClass(Text.class);
		step2Job.setOutputValueClass(FloatWritable.class);

		step2Job.setMapperClass(step2Map.class);
		step2Job.setReducerClass(step2Reduce.class);

		step2Job.setInputFormatClass(TextInputFormat.class);
		step2Job.setOutputFormatClass(TextOutputFormat.class);

//		FileInputFormat.addInputPath(step2Job, new Path("hdfs://master:9000" + args[2]));
		FileInputFormat.addInputPath(step2Job, new Path(args[2]+ "/part-r-00000"));
		FileOutputFormat.setOutputPath(step2Job, new Path(args[3]));

		step2Job.waitForCompletion(true);
		
		
		Job step3Job = Job.getInstance(getConf());
		step3Job.setJarByClass(word2vecDriver.class);
		step3Job.setOutputKeyClass(Text.class);
		step3Job.setOutputValueClass(Text.class);

		step3Job.setMapperClass(step3Map.class);
		step3Job.setReducerClass(step3Reduce.class);

		step3Job.setInputFormatClass(TextInputFormat.class);
		step3Job.setOutputFormatClass(TextOutputFormat.class);

//		FileInputFormat.addInputPath(step3Job, new Path("hdfs://master:9000" + args[3]));
		FileInputFormat.addInputPath(step3Job, new Path(args[3]+ "/part-r-00000"));
		FileOutputFormat.setOutputPath(step3Job, new Path(args[4]));

		step3Job.waitForCompletion(true);
		
		
		return 0;
	}

	/*
	 * STEP 1
	 * Get Mapreduce Key(topAsin + topWord + overallAsin), 
	 * 					Value(topWord's max similarity in overallAsin's all word)
	 */
	public static class step1Map extends Mapper<LongWritable, Text, Text, FloatWritable> {
		Configuration conf;
		String filePath;
		Word2VEC word2vec = new Word2VEC();

		public void setup(Context context) {
			conf = context.getConfiguration();
			filePath = conf.get("filePath");
			
			try {
				word2vec.loadGoogleModel(filePath);
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}

		@Override
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

			String line = value.toString();
			String[] element = line.split(" ");
			String[] topElem = element[0].split("\t");
			
			String[] overallElem = element[1].split("\t");
			String topAsin = topElem[0], topWord = topElem[1], 
					overallAsin = overallElem[0], overallWord = overallElem[1];
			
			Text newKey = new Text(topAsin + " " + topWord  + " " + overallAsin);
			float similarity = word2vec.wordSimilarity(topWord, overallWord);
			
			context.write(newKey, new FloatWritable(similarity));
		}
	}

	public static class step1Reduce extends
			Reducer<Text, FloatWritable, Text, FloatWritable> {

		@Override
		public void reduce(Text key, Iterable<FloatWritable> values,
				Context context) throws IOException, InterruptedException {
			float topSimilarity = -1;
			
			for (FloatWritable val : values) {  
				float temp = val.get();
				topSimilarity = (temp > topSimilarity) ? temp : topSimilarity;
			}	
			context.write(key, new FloatWritable(topSimilarity));
		}
	}
	
	
	
	/*
	 * STEP 2
	 * Get Mapreduce Key(topAsin overallAsin), 
	 * 					Value(topAsin's average similarity with overallAsin)
	 */
	public static class step2Map extends Mapper<LongWritable, Text, Text, FloatWritable> {

		@Override
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

			String line = value.toString();
			String[] element = line.split("\t");
			String[] keyElem = element[0].split(" ");
			float maxSimilarity = Float.parseFloat(element[1]);
			String topAsin = keyElem[0], topWord = keyElem[1], overallAsin = keyElem[2];

			Text newKey = new Text(topAsin + " " + overallAsin);

			context.write(newKey, new FloatWritable(maxSimilarity));
		}
	}

	public static class step2Reduce extends
			Reducer<Text, FloatWritable, Text, FloatWritable> {
		
		@Override
		public void reduce(Text key, Iterable<FloatWritable> values,
				Context context) throws IOException, InterruptedException {
			
			float sumSimilarity = 0, count = 0, averageSimilarity = 0;
			
			for (FloatWritable val : values) {  
				sumSimilarity += val.get();
				count++;
			}	
			averageSimilarity = sumSimilarity / count;
			
			context.write(key, new FloatWritable(averageSimilarity));
		}
	}
	
	
	
	/*
	 * STEP 3
	 * Get Mapreduce Key(topAsin), 
	 * 					Value(TOP 10 in topAsin's similar overallAsin )
	 */
	public static class step3Map extends Mapper<LongWritable, Text, Text, Text> {

		@Override
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

			String line = value.toString();
			String[] element = line.split("\t");
			String[] keyElem = element[0].split(" ");
			String averageSimilarity = element[1];
			String topAsin = keyElem[0], overallAsin = keyElem[1];

			Text newKey = new Text(topAsin);

			context.write(newKey, new Text(overallAsin + " " + averageSimilarity));
		}
	}

	public static class step3Reduce extends
			Reducer<Text, Text, Text, Text> {
		
		@Override
		public void reduce(Text key, Iterable<Text> values,
				Context context) throws IOException, InterruptedException {
			
			PriorityQueue<SimilarObject> priorityQueue = new PriorityQueue<SimilarObject>();
			
			for (Text val : values) {  
				String[] element = val.toString().split(" ");
				String overallAsin = element[0];
				float averageSimilarity =  Float.parseFloat(element[1]);;
				SimilarObject SimTempObj = new SimilarObject(overallAsin, averageSimilarity);
				priorityQueue.offer(SimTempObj);
				if(priorityQueue.size() > 10) {
					priorityQueue.poll();
				}
			}
			
			PriorityQueue<SimilarObject> reversedPriorityQueue = new PriorityQueue<SimilarObject>(priorityQueue.size(), Collections.reverseOrder());
	        reversedPriorityQueue.addAll(priorityQueue);
			
	        int i = 1;
			while(!reversedPriorityQueue.isEmpty()) {
				SimilarObject SimTempObj = reversedPriorityQueue.poll();
				context.write(key, new Text(SimTempObj.toString() + " (" + i + ")"));
				i++;
			}	
		}
	}
	
	
	
}
