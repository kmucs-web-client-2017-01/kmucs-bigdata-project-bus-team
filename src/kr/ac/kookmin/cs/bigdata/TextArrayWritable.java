package kr.ac.kookmin.cs.bigdata;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.Writable;

public class TextArrayWritable extends ArrayWritable
{
	public TextArrayWritable()
	{
		super(Text.class);
	}
	
	public TextArrayWritable(String[] strings)
	{
		super(Text.class);
		Text[] texts = new Text[strings.length];
		for (int i=0;i<strings.length;i++)
			texts[i] = new Text(strings[i]);
		set(texts);
	}
	
//	@Override
//	public void readFields(DataInput in) throws IOException
//	{
//		values = new Writable[in.toString().length()];
//		
//	}
//	
	
	@Override
	public Text[] get()
	{
		return (Text[])super.get();
	}
	
	@Override
	public void write(DataOutput arg0) throws IOException
	{
		for(Text data : get())
		{
			data.write(arg0);
		}
	}
}
