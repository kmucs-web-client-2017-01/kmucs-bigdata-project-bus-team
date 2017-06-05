package kr.ac.kookmin.cs.bigdata.pkh;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import kr.ac.kookmin.cs.bigdata.pkh.WordCount.WordCountMapper;
import kr.ac.kookmin.cs.bigdata.pkh.WordCount.WordCountReducer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.json.JSONException;
import org.json.JSONObject;

public class WordCountForAsin extends Configured implements Tool {

	public static class WordCountForAsinMapper extends
			Mapper<LongWritable, Text, Text, Text> {

		public void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {

			try {
				String[] wordAndAsinCounter = value.toString().split("\t");
				String[] wordAndAsin = wordAndAsinCounter[0].split("@");
				context.write(new Text(wordAndAsin[1]), new Text(wordAndAsin[0]
						+ "=" + wordAndAsinCounter[1]));
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
	}

	public static class WordCountForAsinReducer extends
			Reducer<Text, Text, Text, Text> {

		public void reduce(Text key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {

			int sumofWordsInAsin = 0;
			Map<String, Integer> tempCounter = new HashMap<String, Integer>();
			for (Text val : values) {
				String[] wordCounter = val.toString().split("=");
				tempCounter
						.put(wordCounter[0], Integer.valueOf(wordCounter[1]));
				sumofWordsInAsin += Integer
						.parseInt(val.toString().split("=")[1]);
			}
			for (String wordKey : tempCounter.keySet()) {
				context.write(new Text(wordKey + "@" + key.toString()),
						new Text(tempCounter.get(wordKey) + "/"
								+ sumofWordsInAsin));
			}
		}
	}

	public int run(String[] args) throws Exception {

		Configuration conf = new Configuration(true);
		conf.set("fs.default.name", "hdfs://" + "master" + ":9000");

		Job wordCountForAsin = new Job(conf, "WordCountForAsin");
		wordCountForAsin.setJarByClass(WordCountForAsinMapper.class);
		wordCountForAsin.setOutputKeyClass(Text.class);
		wordCountForAsin.setOutputValueClass(Text.class);

		wordCountForAsin.setMapperClass(WordCountForAsinMapper.class);
		wordCountForAsin.setReducerClass(WordCountForAsinReducer.class);

		wordCountForAsin.setInputFormatClass(TextInputFormat.class);
		wordCountForAsin.setOutputFormatClass(TextOutputFormat.class);

		FileInputFormat.addInputPath(wordCountForAsin, new Path(
				Tfidf.INPUTPATH2));
		FileOutputFormat.setOutputPath(wordCountForAsin, new Path(
				Tfidf.OUTPUTPATH2));

		wordCountForAsin.waitForCompletion(true);

		return 0;
	}
}
