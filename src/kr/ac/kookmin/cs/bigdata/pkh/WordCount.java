package kr.ac.kookmin.cs.bigdata.pkh;

import java.io.IOException;
import java.util.ArrayList;

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

public class WordCount extends Configured implements Tool {
	public static class WordCountMapper extends
			Mapper<LongWritable, Text, Text, IntWritable> {
		final static int MINIMUM_LENGTH = 10 ;
		String asin = new String();
		String description = new String();

		public void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {

			JSONObject jsonObj;

			try {
				jsonObj = new JSONObject(value.toString());
				if (jsonObj.has("description")) {
					description = jsonObj.get("description").toString();
					if (description.length() < MINIMUM_LENGTH )
						return ;
					asin = jsonObj.get("asin").toString();
				} else
					return;
			} catch (JSONException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}

			ArrayList<String> listDescription = Preprocessing.removeNeedlessWords(description) ;
			
			for ( String val : listDescription){
				context.write(new Text(val.toLowerCase() + "@" + asin), new IntWritable(1));
			}
			
		}
	}

	public static class WordCountReducer extends
			Reducer<Text, IntWritable, Text, IntWritable> {

		public void reduce(Text key, Iterable<IntWritable> values,
				Context context) throws IOException, InterruptedException {

			int sum = 0;
			for (IntWritable val : values) {
				sum += val.get();
			}
			context.write(key, new IntWritable(sum));
		}

	}
	
	public int run(String[] args) throws Exception {

		Configuration conf = new Configuration(true);

		Job wordCount = new Job(conf, "WordCount");
		wordCount.setJarByClass(WordCountMapper.class);
		wordCount.setOutputKeyClass(Text.class);
		wordCount.setOutputValueClass(IntWritable.class);

		wordCount.setMapperClass(WordCountMapper.class);
		wordCount.setReducerClass(WordCountReducer.class);

		wordCount.setInputFormatClass(TextInputFormat.class);
		wordCount.setOutputFormatClass(TextOutputFormat.class);

		FileInputFormat.addInputPath(wordCount, new Path(Tfidf.INPUTPATH));
		FileOutputFormat.setOutputPath(wordCount, new Path(Tfidf.OUTPUTPATH));

		wordCount.waitForCompletion(true);

		return 0;
	}
}