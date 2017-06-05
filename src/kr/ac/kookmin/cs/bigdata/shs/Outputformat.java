package kr.ac.kookmin.cs.bigdata;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

public class Outputformat implements Writable
{
	private Text salesRank, title;
	private TextArrayWritable description;
	
	public Outputformat()
	{
		this.salesRank = new Text();
		this.title = new Text();
		this.description = new TextArrayWritable();
	}
	
	public Outputformat(Text inputSalesRank, Text inputTitle, ArrayList<String> inputDescription)
	{
		this.salesRank = inputSalesRank;
		this.title = inputTitle;
		Object[] objList = inputDescription.toArray();
		String[] stringArray = Arrays.copyOf(objList,  objList.length, String[].class);
		this.description = new TextArrayWritable(stringArray);
	}

	public void Set(Text inputSalesRank, Text inputTitle, ArrayList<String> inputDescription)
	{
		this.salesRank = inputSalesRank;
		this.title = inputTitle;
		Object[] objList = inputDescription.toArray();
		String[] stringArray = Arrays.copyOf(objList,  objList.length, String[].class);
		this.description = new TextArrayWritable(stringArray);
		
	}
	
	public Text GetSalesRank()
	{
		return this.salesRank;
	}
	
	public Text GetTitle()
	{
		return this.title;
	}
	
	public TextArrayWritable Getdescription()
	{
		return this.description;
	}
	
	@Override
	public void readFields(DataInput in) throws IOException
	{
		salesRank.readFields(in);
		title.readFields(in);
		description.readFields(in);
	}
	
	@Override
	public void write(DataOutput out) throws IOException
	{
		salesRank.write(out);
		title.write(out);
		description.write(out);
	}
	
	@Override
	public String toString()
	{
		String str = "salesRank : " + salesRank.toString() + "\n";
		str += "title : " + title.toString() + "\n";
		str += "description : ";
		String[] strings = description.toStrings();
		for(int i=0;i<strings.length;i++)
			str += " " + strings[i];
		System.out.println();
		return str;
	}
}
