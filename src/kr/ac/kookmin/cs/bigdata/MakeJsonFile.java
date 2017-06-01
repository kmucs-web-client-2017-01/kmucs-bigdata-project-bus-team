package kr.ac.kookmin.cs.bigdata;


import java.io.IOException;
import java.util.Arrays;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.json.*;

public class MakeJsonFile extends Configured implements Tool
{
    public static void main(String[] args) throws Exception
    {
        System.out.println(Arrays.toString(args));
        int res = ToolRunner.run(new Configuration(), new MakeJsonFile(), args);
      
        System.exit(res);
    }

    @Override
    public int run(String[] args) throws Exception 
    {
        System.out.println(Arrays.toString(args));

        Job job = Job.getInstance(getConf());
        job.setJarByClass(MakeJsonFile.class);
        
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
        private Text word1 = new Text();
        private Text word2 = new Text();

        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException 
        {
        	try
			{
				JSONObject jsonObj = new JSONObject(value.toString());
				if(jsonObj.has("description"))
				{
					String temp = jsonObj.get("description").toString();
					
					if(temp != null)
					{
						for (String token: temp.split("\\s+"))
						{
							if(token.equals(temp) == true)
								continue;

							word1.set(temp);
							word2.set(token);
							context.write(word1, word2);
						}
					}
				}
			}
			catch(JSONException e)
			{
				e.printStackTrace();
			}
        }
    }

    public static class Reduce extends Reducer<Text, Text, Text, NullWritable>
    {
        @Override
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException 
        {
            JSONObject jsn = new JSONObject();
            JSONArray arr = new JSONArray();
            
            for (Text val : values)
            	arr.put(val.toString());
            
            try
            {
            	jsn.put("asin", key);
            	jsn.put("descriptionWord",arr);
			}
            
            catch (JSONException e)
            {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
            
            context.write(new Text(jsn.toString()), null);
        }
    }
}