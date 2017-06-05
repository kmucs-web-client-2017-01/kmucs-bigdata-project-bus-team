package kr.ac.kookmin.cs.bigdata.pkh;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;


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

public class TopwordInTFIDF extends Configured implements Tool {
	public static class TopwordInTFIDFMapper extends
			Mapper<LongWritable, Text, Text, Text> {

		public void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {

			String[] wordAndCounters = value.toString().split("\t");
			String[] wordAndDoc = wordAndCounters[0].split("@"); // 3/1500
			context.write(new Text(wordAndDoc[1]), new Text(wordAndDoc[0] + "@"
					+ wordAndCounters[1]));

		}
	}

	public static class TopwordInTFIDFReducer extends
			Reducer<Text, Text, Text, Text> {
		private String word;
		private Double tfidf;
		private static int RANKSIZE = 5 ;
		
		public void reduce(Text key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {

			HashMap<String, Double> map = new HashMap<String, Double>();
			String rankword = "" ;
			String ranktfidf = "" ;
			int rankCount = 0 ;
			
			for (Text val : values) {
				String[] wordandTfidf = val.toString().split("@");
				word = wordandTfidf[0];
				tfidf = Double.parseDouble(wordandTfidf[1]);
				map.put(word, tfidf);
			}

			 Iterator it = sortByValue(map).iterator();
	            while (it.hasNext()) {
	                rankCount++;
	                String Key = (String) it.next();
	                context.write(new Text(key), new Text(Key) );
	                if (rankCount == RANKSIZE)
	                    break;
	            }
		}
	}

	public static List<Integer> sortByValue(final HashMap map) {
		List<Integer> list = new ArrayList<Integer>();
		list.addAll(map.keySet());
		Collections.sort(list, new Comparator<Object>() {

			@SuppressWarnings("unchecked")
			public int compare(Object o1, Object o2) {
				Object v1 = map.get(o1);
				Object v2 = map.get(o2);
				return ((Comparable<Object>) v1).compareTo(v2);
			}
		});
		Collections.reverse(list);
		return list;
	}

	public int run(String[] args) throws Exception {

		Configuration conf = new Configuration(true);
		conf.set("fs.default.name", "hdfs://" + "master" + ":9000") ;
		
		Job topword = new Job(conf, "topword");
		topword.setJarByClass(TopwordInTFIDFMapper.class);
		topword.setOutputKeyClass(Text.class);
		topword.setOutputValueClass(Text.class);

		topword.setMapperClass(TopwordInTFIDFMapper.class);
		topword.setReducerClass(TopwordInTFIDFReducer.class);

		topword.setInputFormatClass(TextInputFormat.class);
		topword.setOutputFormatClass(TextOutputFormat.class);

		FileInputFormat.addInputPath(topword, new Path(Tfidf.INPUTPATH4));
		FileOutputFormat.setOutputPath(topword, new Path(Tfidf.OUTPUTPATH4));

		topword.waitForCompletion(true);

		return 0;
	}
}
