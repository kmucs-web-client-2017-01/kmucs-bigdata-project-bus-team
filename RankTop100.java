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
import org.json.*;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;


public class RankTop100 {

	public static void main(String[] args){
		
		if (args.length == 0) {                   // args.length 는 옵션 개수
		      System.err.println("Input Filename...");
		      System.exit(1);                         // 읽을 파일명을 주지 않았을 때는 종료
		}
		
		System.out.println(Arrays.toString(args));
		
		try {
			
			String line = readFileAsString(args[0]);
			
			setrank(line, args[1]);
			
		} catch (IOException | JSONException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	public static void setrank(String line, String outpath) throws JSONException, IOException{
		
		Map<String, Integer> asinRank = new HashMap<String, Integer>();
		
		String[] tuple = line.split("\\n");
		JSONObject obj, tempObj;
		for (int i = 0; i < tuple.length; i++) {
			obj = new JSONObject(tuple[i]);
			if(!obj.has("salesRank"))
				continue;
			String asin = obj.get("asin").toString();
			String temprank = obj.get("salesRank").toString();
			
			tempObj = new JSONObject(temprank);
			
			int rank = tempObj.getInt("Books");
			
			if(rank > 0 && rank <= 5000000)
				asinRank.put(asin, rank);
		}
		
		Iterator it = sortByValue(asinRank).iterator();
		int count = 0;
		
		FileWriter file = new FileWriter(outpath);
		JSONObject obj_temp = new JSONObject();
		while(it.hasNext()){
			
			if(count == 100)
				break;
			String asin_str = it.next().toString();
			obj_temp.put("asin", asin_str);
			obj_temp.put("salesRank", asinRank.get(asin_str));
			
			System.out.print(obj_temp.toString() + "\n");
			
			file.write(obj_temp.toString() + "\n");
			file.flush();
				
			count++;
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
