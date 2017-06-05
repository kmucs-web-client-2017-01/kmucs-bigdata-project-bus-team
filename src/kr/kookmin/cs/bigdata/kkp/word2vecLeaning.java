package kr.kookmin.cs.bigdata.kkp;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.DoubleWritable;
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

import kr.kookmin.cs.bigdata.kkp.vec.Word2VEC;



public class word2vecLeaning  {
	
	private static Word2VEC word2vec = new Word2VEC();
	public static void main(String[] args) throws Exception {
		String FILE = "frWiki_non_lem.txt";
		System.out.println("Start....");
		
		try {
			Word2VEC.trainModel(FILE, "en_3G_trained.bin");
			System.out.println("Finish...");
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
}
