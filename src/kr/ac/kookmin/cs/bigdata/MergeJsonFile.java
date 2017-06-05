package kr.ac.kookmin.cs.bigdata;

/*
 * 
 * This Java class has the ability to integrate two json files.
 * The form is as follows.
 * <AsinOfTop wordOfTop asinOfOverall wordOfOverall>
 *
 */

import java.io.IOException;
import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import org.json.JSONException;
import org.json.JSONObject;

public class MergeJsonFile
{
	public static void main(String[] args) throws IOException, InterruptedException, JSONException
	{
		FetchTopJsonFile("hdfs dfs -get topFile.txt /student2/topfile");
		FetchOverallJsonFile("hdfs dfs -get overallFile_sample.txt /student2/overallfile");
		MergeTopAndOverallFile("topFile.txt", "overallFile.txt");
		UploadFileMerged("hdfs dfs -put mergedFile.txt /student2/");
	}
	
	
	// This function downloads top100 json file on hdfs to local area.
	public static void FetchTopJsonFile(String command) throws IOException, InterruptedException
	{
		System.out.println("Get Top100 Json File Start");
		Process proc = Runtime.getRuntime().exec(command);  //command will be hdfs dfs -get ~~
		proc.waitFor();
		System.out.println("Get Top100 Json File Finished!!!!!");
	}
	
	
	// This function downloads overall json file on hdfs to local area.
	public static void FetchOverallJsonFile(String command) throws IOException, InterruptedException
	{
		System.out.println("Get Overall Json File Start");
		Process proc = Runtime.getRuntime().exec(command);  //command will be hdfs dfs -get ~~
		proc.waitFor();
		System.out.println("Get Overall Json File Finished!!!!!");
	}
	
	// This function uploads the merged file to hdfs.
	public static void UploadFileMerged(String command) throws IOException, InterruptedException
	{
		System.out.println("Upload Json File to hdfs Start");
		Process proc = Runtime.getRuntime().exec(command);  //command will be hdfs dfs -put ~~
		proc.waitFor();
		System.out.println("Upload Json File to hdfs Finished!!!!!");
	}
	
	public static void MergeTopAndOverallFile(final String topJsonFilePath, final String overallJsonFilePath) throws IOException, JSONException
	{
		BufferedReader topIn = new BufferedReader(new FileReader(topJsonFilePath));
		BufferedWriter fw = new BufferedWriter(new FileWriter("mergedFile.txt", true));
		
		try 
		{
			String topString, overallString;
			
			while ((topString = topIn.readLine()) != null)
			{
				JSONObject jsonObj = new JSONObject(topString);
				//String topFilterString = "";
				//String descriptionWords = jsonObj.get("description").toString();
				//topFilterString += jsonObj.getString("asin");
				
				//for(String token : descriptionWords.split(" "))
				//{
					BufferedReader overallIn = new BufferedReader(new FileReader(overallJsonFilePath));
					
					while((overallString = overallIn.readLine()) != null)
					{
						//fw.write(topString + " " + token + " " + overallString);
						fw.write(topString + overallString);
						fw.flush();
					}
					overallIn.close();
				//}
			}
			fw.close();
		}
		
		catch (FileNotFoundException e)
		{
			e.printStackTrace();
		}
	}
}
