package kr.ac.kookmin.cs.bigdata;

/*
 * This java file extracts only the necessary information (asin, salesRank, title, description) from the existing file and creates it as a json file.
 */


import java.io.IOException;
import java.util.Arrays;
import java.util.ArrayList;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.ObjectWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
//import org.codehaus.jettison.json.JSONArray;
//import org.codehaus.jettison.json.JSONException;
//import org.codehaus.jettison.json.JSONObject;
import org.json.*;



public class MakeJsonFile extends Configured implements Tool
{
	public static void main(String[] args) throws Exception
	{
		System.out.println(Arrays.toString(args));
		int res = ToolRunner.run(new Configuration(), new MakeJsonFile(), args);
		System.exit(res);
	}


	public int run(String[] args) throws Exception 
	{
		System.out.println(Arrays.toString(args));

		Job job = Job.getInstance(getConf());
		job.setJarByClass(MakeJsonFile.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Outputformat.class);

		job.setMapperClass(Map.class);
		job.setReducerClass(Reduce.class);

		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);

		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		job.waitForCompletion(true);

		return 0;
	}

	public static class Map extends Mapper<LongWritable, Text, Text, Outputformat>
	{
		private Text keyAsin = new Text();
		private ObjectWritable property = new ObjectWritable();

		
		@Override
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException 
		{
			try
			{				
				JSONObject jsonObj = new JSONObject(value.toString());
				if(jsonObj.has("description") && jsonObj.has("salesRank") && jsonObj.has("description") && jsonObj.has("title"))
				{
					String asin = jsonObj.get("asin").toString();
					String salesRank = jsonObj.getJSONObject("salesRank").toString();
					String description = jsonObj.get("description").toString();
					String title = jsonObj.getString("title").toString();

					ArrayList<String> modifiedDescription = Preprocessing.removeNeedlessWords(description);
					Outputformat out = new Outputformat(new Text(salesRank), new Text(title), modifiedDescription);
					keyAsin.set(asin);
					context.write(keyAsin, out);
				}
			}
			catch(JSONException e)
			{
				e.printStackTrace();
			}
		}
	}

	public static class Reduce extends Reducer<Text, Outputformat, Text, NullWritable>
	{
		@Override
		public void reduce(Text key, Iterable<Outputformat> values, Context context) throws IOException, InterruptedException 
		{
			JSONObject jsn = new JSONObject();
			JSONArray arr = new JSONArray();
			JSONObject book = new JSONObject();
			String str = "";
			String salesRank = "";
			String title = "";
			try
			{
				for(Outputformat val : values)
				{
					salesRank = val.GetSalesRank().toString();
					book.put("Books", salesRank.substring(salesRank.indexOf(":")+1, salesRank.length()-1));
					title = val.GetTitle().toString();
					String description = val.Getdescription().toString();
					
					for (String token: description.split("\\s+"))
					{
						if(!str.contains(token))
						{
							str += token + " ";
							arr.put(token);
						}
					}
				}
				jsn.put("asin", key);
				jsn.put("salesRank", book);
				jsn.put("title", title);
				jsn.put("descriptionWord",arr);
				context.write(new Text(jsn.toString()), null);
			}
			catch (JSONException e)
			{
				// TODO Auto-generated catch block
				e.printStackTrace();
			}			
		}
	}
}