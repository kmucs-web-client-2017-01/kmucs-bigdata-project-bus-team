package kr.ac.kookmin.cs.bigdata;

import java.io.IOException;
import java.util.HashSet;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;

public class AsinCount extends Configured implements Tool{
	
	public static class AsinCountMapper extends
			Mapper<LongWritable, Text, Text, Text> {
		
		public void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {

			String[] wordAndAsinCounter = value.toString().split("\t") ;
			String[] wordAndAsin = wordAndAsinCounter[0].split("@") ;
			context.write(new Text("1"), new Text(wordAndAsin[1])) ;
			
		}
	}

	public static class AsinCountReducer extends
			Reducer<Text, Text, Text, Text> {

		public void reduce(Text key, Iterable<Text> values,
				Context context) throws IOException, InterruptedException {

	        HashSet<String> map_list = new HashSet<String>();
	        for (Text val : values) {
	        	map_list.add(val.toString()) ;
	        }
	        Tfidf.numofAsin = map_list.size();
	      //  context.write(new Text(""), new IntWritable(Tfidf.numofAsin )) ;
		}
	}

	public int run(String[] args) throws Exception {

		Configuration conf = new Configuration(true);

		Job asinCount = new Job(conf, "asinCount");
		asinCount.setJarByClass(AsinCountMapper.class);
		asinCount.setOutputKeyClass(Text.class);
		asinCount.setOutputValueClass(Text.class);

		asinCount.setMapperClass(AsinCountMapper.class);
		asinCount.setReducerClass(AsinCountReducer.class);

		asinCount.setInputFormatClass(TextInputFormat.class);
		asinCount.setOutputFormatClass(TextOutputFormat.class);

		FileInputFormat.addInputPath(asinCount, new Path(Tfidf.INPUTPATH2));
		FileOutputFormat.setOutputPath(asinCount, new Path(Tfidf.OUTPUTPATH5));

		asinCount.waitForCompletion(true);
		
		return 0;
	}
}

