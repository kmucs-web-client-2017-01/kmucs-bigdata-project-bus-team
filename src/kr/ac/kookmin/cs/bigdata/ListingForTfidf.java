package kr.ac.kookmin.cs.bigdata;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;

import org.json.JSONException;
import org.json.JSONObject;

public class ListingForTfidf
{
	String asins;
	
	public void ReadJsonFile(String file) throws IOException, JSONException
	{
		BufferedReader in = new BufferedReader(new FileReader(file));
		String line;
		
		while((line = in.readLine()) != null)
		{
			JSONObject jsonObj = new JSONObject(line);
			String asin = jsonObj.get("asin").toString();
			asins += asin + " ";
		}
		
	}
	
	public void ReadFileAndClassifier(String file) throws IOException
	{
		BufferedWriter isHave = new BufferedWriter(new FileWriter("IsHaveAsin.txt", true));
		BufferedWriter isNotHave = new BufferedWriter(new FileWriter("IsNotHaveAsin.txt", true));
		BufferedReader in = new BufferedReader(new FileReader(file));
		String line;
		
		while((line = in.readLine()) != null)
		{
			String[] temp = line.split("\t");
			if(asins.contains(temp[0]))
			{
				isHave.write(temp[0] + " " + temp[1] + "\n");
				isHave.flush();
			}
			else
			{
				isNotHave.write(temp[0] + " " + temp[1] + "\n");
				isNotHave.flush();
			}
		}
		in.close();
	}
	
	public static void main(String[] args) throws IOException, JSONException
	{
		ListingForTfidf listForTfidf = new ListingForTfidf();
		listForTfidf.ReadJsonFile("rank-top3.json");
		listForTfidf.ReadFileAndClassifier("tfidf-row100");
	}
}
