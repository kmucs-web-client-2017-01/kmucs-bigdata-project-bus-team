package kr.ac.kookmin.cs.bigdata;

import java.io.IOException;
import java.text.DecimalFormat;
import java.util.HashMap;
import java.util.Map;

import kr.ac.kookmin.cs.bigdata.WordCount.WordCountMapper;
import kr.ac.kookmin.cs.bigdata.WordCount.WordCountReducer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
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

public class CalculateTFIDF extends Configured implements Tool {
	
	public static class ProcessTFIDFMapper extends
			Mapper<LongWritable, Text, Text, Text> {

		public void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {

			 String[] wordAndCounters = value.toString().split("\t");
		     String[] wordAndDoc = wordAndCounters[0].split("@");                 //3/1500
		     context.write(new Text(wordAndDoc[0]), new Text(wordAndDoc[1] + "=" + wordAndCounters[1]));

		}
	}

	public static class ProcessTFIDFReducer extends
			Reducer<Text, Text, Text, Text> {
		private static final DecimalFormat DF = new DecimalFormat("###.#######");
		 
		public void reduce(Text key, Iterable<Text> values,
				Context context) throws IOException, InterruptedException {

			 // get the number of documents indirectly from the file-system (stored in the job name on purpose)
	        int numberOfDocumentsInCorpus = Tfidf.numofAsin ;
	        // total frequency of this word
	        int numberOfDocumentsInCorpusWhereKeyAppears = 0;
	        Map<String, String> tempFrequencies = new HashMap<String, String>();
	        for (Text val : values) {
	            String[] documentAndFrequencies = val.toString().split("=");
	            numberOfDocumentsInCorpusWhereKeyAppears++;
	            tempFrequencies.put(documentAndFrequencies[0], documentAndFrequencies[1]);
	        }
	        for (String document : tempFrequencies.keySet()) {
	            String[] wordFrequenceAndTotalWords = tempFrequencies.get(document).split("/");
	 
	            //Term frequency is the quocient of the number of terms in document and the total number of terms in doc
	            double tf = Double.valueOf(Double.valueOf(wordFrequenceAndTotalWords[0])
	                    / Double.valueOf(wordFrequenceAndTotalWords[1]));
	 
	            //interse document frequency quocient between the number of docs in corpus and number of docs the term appears
	            double idf = (double) numberOfDocumentsInCorpus / (double) numberOfDocumentsInCorpusWhereKeyAppears;
	 
	            //given that log(10) = 0, just consider the term frequency in documents
	            double tfIdf = numberOfDocumentsInCorpus == numberOfDocumentsInCorpusWhereKeyAppears ?
	                    tf : tf * Math.log10(idf);
	 
//	            context.write(new Text(key + "@" + document), new Text("[" + numberOfDocumentsInCorpusWhereKeyAppears + "/"
//	                    + numberOfDocumentsInCorpus + " , " + wordFrequenceAndTotalWords[0] + "/"
//	                    + wordFrequenceAndTotalWords[1] + " , " + DF.format(tfIdf) + "]"));
	            
	            context.write(new Text(key + "@" + document), new Text(DF.format(tfIdf)));
	        }
		}
	}

	public int run(String[] args) throws Exception {

		Configuration conf = new Configuration(true);

		Job calTFIDF = new Job(conf, "calTFIDF");
		calTFIDF.setJarByClass(ProcessTFIDFMapper.class);
		calTFIDF.setOutputKeyClass(Text.class);
		calTFIDF.setOutputValueClass(Text.class);

		calTFIDF.setMapperClass(ProcessTFIDFMapper.class);
		calTFIDF.setReducerClass(ProcessTFIDFReducer.class);

		calTFIDF.setInputFormatClass(TextInputFormat.class);
		calTFIDF.setOutputFormatClass(TextOutputFormat.class);

		FileInputFormat.addInputPath(calTFIDF, new Path(Tfidf.INPUTPATH3));
		FileOutputFormat.setOutputPath(calTFIDF, new Path(Tfidf.OUTPUTPATH3));

		
		calTFIDF.waitForCompletion(true);

		return 0;
	}
}

