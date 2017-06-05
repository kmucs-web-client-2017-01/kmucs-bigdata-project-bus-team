package kr.ac.kookmin.cs.bigdata;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.conf.Configured;
import org.json.*;


public class RankTop100 extends Configured {
	public static void main(String[] args) throws Exception{
		
		if (args.length == 0) {                   // args.length 는 옵션 개수
		      System.err.println("Input Filename...");
		      System.exit(1);                         // 읽을 파일명을 주지 않았을 때는 종료
		}
		
		System.out.println(Arrays.toString(args));
	
		//String line = readFileAsString(args[0]);
			
		setrank(args[0], args[1]);
	}
	
	public static void setrank(String inpath, String outpath) throws IOException{
		
		Map<String, Integer> asinRank = new HashMap<String, Integer>();
		//Map<String, String> asinDesc = new HashMap<String, String>();		//if include description
		
		BufferedReader in = new BufferedReader(new FileReader(inpath));
	      String s;
		
		//String[] tuple = line.split("\\n");
		JSONObject obj, tempObj;
		while ((s = in.readLine()) != null) {
			try{
			//System.out.println("sssssssssssssss : " + s);
			obj = new JSONObject(s);
			if(!obj.has("salesRank"))
				continue;
			//if(!obj.has("salesRank") || !obj.has("description"))	//if include description
				//continue;
			String asin = obj.get("asin").toString();
			String temprank = obj.get("salesRank").toString();
			//String tempdesc = obj.get("description").toString();	//if include description
			
			tempObj = new JSONObject(temprank);
			
			int rank = tempObj.getInt("Books");
			
			if(rank > 0 && rank <= 100000){
				asinRank.put(asin, rank);
				//asinDesc.put(asin, tempdesc);				// if include description
			}
		}catch (JSONException e){
			System.out.println("ssssssssssssssssssss" + s);
			e.printStackTrace();
		}
		}
		
		//@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@
		
		Iterator it = sortByValue(asinRank).iterator();
		int count = 0;
		
		FileWriter file = new FileWriter(outpath);
		JSONObject obj_temp = new JSONObject();
		while(it.hasNext()){
			try{
			if(count == 100)
				break;
			String asin_str = it.next().toString();
			obj_temp.put("asin", asin_str);
			obj_temp.put("salesRank", asinRank.get(asin_str));
			//obj_temp.put("description", asinDesc.get(asin_str));	//if include description
			
			System.out.print(obj_temp.toString() + "\n");
			
			file.write(obj_temp.toString() + "\n");
			file.flush();
				
			count++;
			
			}catch(JSONException e){
				e.printStackTrace();
			}
		}
		
		file.close();
		
		asinRank.clear();
		
		System.out.println("finish");
	
	}
	
	public static List sortByValue(final Map map){
		List<String> list = new ArrayList();
		list.addAll(map.keySet());

		Collections.sort(list,new Comparator(){
			public int compare(Object o1,Object o2){
				Object v1 = map.get(o1);
				Object v2 = map.get(o2);
				return ((Comparable) v1).compareTo(v2);
				}});
		//Collections.reverse(list);
		return list;
	}
	
	private static String readFileAsString(String filePath) throws IOException {
        StringBuffer fileData = new StringBuffer();
        BufferedReader reader = new BufferedReader(
                new FileReader(filePath));
        char[] buf = new char[1024];
        int numRead=0;
        while((numRead=reader.read(buf)) != -1){
            String readData = String.valueOf(buf, 0, numRead);
            fileData.append(readData);
        }
        reader.close();
        return fileData.toString();
    }
}
