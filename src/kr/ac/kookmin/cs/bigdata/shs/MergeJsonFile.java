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


public class MergeJsonFile
{
	public static void main(String[] args) throws IOException, InterruptedException
	{
		MergeJsonFile mjf = new MergeJsonFile();
		MergeTopAndOverallFile("top100Books", "tfidf-row100");
	}

	public static void MergeTopAndOverallFile(final String topJsonFilePath, final String overallJsonFilePath) throws IOException
	{
		BufferedReader topIn = new BufferedReader(new FileReader(topJsonFilePath));
		BufferedWriter fw = new BufferedWriter(new FileWriter("mergedFile.txt", true));

		try
		{
			String topString, overallString;

			while ((topString = topIn.readLine()) != null)
			{
				BufferedReader overallIn = new BufferedReader(new FileReader(overallJsonFilePath));

				while((overallString = overallIn.readLine()) != null)
				{
					fw.write(topString + " " + overallString + "\n");
					fw.flush();
				}
				overallIn.close();
			}
			fw.close();
		}

		catch (FileNotFoundException e)
		{
			e.printStackTrace();
		}
	}
}
