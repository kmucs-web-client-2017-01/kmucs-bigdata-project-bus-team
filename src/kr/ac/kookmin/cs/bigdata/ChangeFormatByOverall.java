package kr.ac.kookmin.cs.bigdata;


/*
 * 
 *  This Java file will only create a new version of the json file with an overall 3.5 or higher.
 *	The form is as follows.
 *  (Asin, asin-wordDescription)
 * 
 */


import java.io.IOException;
import java.util.Arrays;
import java.util.ArrayList;

import kr.ac.kookmin.cs.bigdata.MakeJsonFile.Map;
import kr.ac.kookmin.cs.bigdata.MakeJsonFile.Reduce;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.ObjectWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.codehaus.jettison.json.JSONArray;
import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;

public class ChangeFormatByOverall  extends Configured implements Tool
{
	public static void main(String[] args) throws Exception
	{
		System.out.println(Arrays.toString(args));
		int res = ToolRunner.run(new Configuration(), new ChangeFormatByOverall(), args);
		System.exit(res);
	}


	public int run(String[] args) throws Exception 
	{
		System.out.println(Arrays.toString(args));
		Job job = Job.getInstance(getConf());
		
		job.setJarByClass(ChangeFormatByOverall.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		job.setMapperClass(Map.class);
		job.setReducerClass(Reduce.class);

		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);

		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		job.waitForCompletion(true);

		return 0;
	}

	public static class Map extends Mapper<LongWritable, Text, Text, Text>
	{
		private Text keyAsin = new Text();
		private Text description = new Text();  //asin+descriptionWord

		@Override
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException 
		{
			try
			{
				JSONObject jsonObj = new JSONObject(value.toString());
				if(jsonObj.has("description"))
				{
					String asin = jsonObj.get("asin").toString();
					String descriptions = jsonObj.get("description").toString();
					keyAsin.set(asin);
					
					for (String token : descriptions.split(" "))
					{
						description.set(asin + " " + token);
						context.write(keyAsin, description);
					}
				}
			}
			catch(JSONException e)
			{
				e.printStackTrace();
			}
		}
	}

	public static class Reduce extends Reducer<Text, Text, NullWritable, Text>
	{
		@Override
		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException 
		{
			String str = "";
			for(Text iter : values)
			{
				if(!str.contains(iter.toString()))
				{
					str += iter.toString();
					context.write(null, iter);
				}
			}
		}
	}
}
