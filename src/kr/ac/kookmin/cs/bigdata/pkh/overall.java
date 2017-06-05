package kr.ac.kookmin.cs.bigdata.pkh;


import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Arrays;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
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

public class overall extends Configured implements Tool {
	final static String RESULT = getNowPath() + "/output/part-r-00000" ;
	final static String META_JSON = getNowPath() + "/input2.json";
	final static String NEW_MEATDATA = getNowPath() +"/new-meta-1000.json";

	public static String getNowPath() {
		return System.getProperty("user.dir") ;
	}
	
	public static void main(String[] args) throws Exception {
		System.out.println(Arrays.toString(args));
		int res = ToolRunner.run(new Configuration(), new overall(), args);

		System.exit(res);
	}

	@Override
	public int run(String[] args) throws Exception {
		System.out.println(Arrays.toString(args));

		Job job = Job.getInstance(getConf());
		job.setJarByClass(overall.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(DoubleWritable.class);

		job.setMapperClass(Map.class);
		job.setReducerClass(Reduce.class);

		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);

		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		job.waitForCompletion(true);
		
		refreshJSON() ;
		
		return 0;
	}

	public static class Map extends
			Mapper<LongWritable, Text, Text, DoubleWritable> {
		private String asin = new String();
		private Double overall;

		@Override
		public void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
			JSONObject obj;
			
			try{
				obj = new JSONObject(value.toString());
				asin = obj.get("asin").toString();
				overall = Double.parseDouble(obj.get("overall").toString());

			}
			catch(JSONException e){
				e.printStackTrace() ;
			}
			context.write(new Text(asin), new DoubleWritable(overall));

		}
	}

	private static String readFileAsString(String filePath) throws IOException {
		StringBuffer fileData = new StringBuffer();
		BufferedReader reader = new BufferedReader(new FileReader(filePath));
		char[] buf = new char[1024];
		int numRead = 0;
		while ((numRead = reader.read(buf)) != -1) {
			String readData = String.valueOf(buf, 0, numRead);
			fileData.append(readData);
		}
		reader.close();
		return fileData.toString();
	}

	public static class Reduce extends
			Reducer<Text, DoubleWritable, Text, NullWritable> {
		private String asin = new String();

		@Override
		public void reduce(Text key, Iterable<DoubleWritable> values,
				Context context) throws IOException, InterruptedException {
			JSONObject obj = new JSONObject() ;
			double sum = 0;
			int count = 0;
			for (DoubleWritable val : values) {
				sum += val.get();
				count++;
			}
			sum /= (double) count;
			try {
				obj.put("asin", key.toString());
				obj.put("overall", sum) ;
			} catch (JSONException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			context.write(new Text(obj.toString()), null) ;
			
		}
	}
	
	public static void refreshJSON() throws JSONException, IOException {
		JSONObject meta_obj = null;
		JSONObject review_obj = null ;
		String meta_line = readFileAsString(META_JSON);
		String review_line = readFileAsString(RESULT) ;
		String meta_data[] = meta_line.split("\\n");
		String review_data[] = review_line.split("\\n");
		FileWriter fileWriter = new FileWriter(NEW_MEATDATA);

		for (int i = 0; i < meta_data.length; i++) {
			meta_obj = new JSONObject(meta_data[i]);
			
			for(int j = 0 ; j < review_data.length ; j++){
				review_obj = new JSONObject(review_data[j]);
				if(meta_obj.get("asin").toString().compareTo(review_obj.get("asin").toString()) == 0){
					meta_obj.put("overall", review_obj.get("overall").toString());
					fileWriter.write(meta_obj.toString());
					break ;
				}
			}
		}
//		fileWriter.flush();
		fileWriter.close();
	}
}
