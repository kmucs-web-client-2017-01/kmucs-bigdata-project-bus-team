package kr.kookmin.cs.bigdata.kkp;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

public class Preprocessing {

	public static final HashMap<String, Integer> PREWORDMAP = new HashMap<String, Integer>(){{
		for(String w : PREWORDLIST) put(w, 1); 
    }};
	private static final String[] PREWORDLIST = {
		"A", "a", "The", "the", "An", "an",
		"is", "Is", "Are", "are", "am", "Am",
		"and", "And", "or", "Or", "to", "To",
		"for", "For", "of", "Of", "on", "On",
		"in", "In", "with", "With", "this", "This",
		"that", "That" 
	};
	public ArrayList<String> removeNeedlessWords(String text)
	{
		ArrayList<String> words = new ArrayList<String>();
		text.replaceAll("[^a-zA-Z0-9\\s]", "");
		String[] splitText = text.split(" ");
		
		for(String w : splitText) {
			if(!PREWORDMAP.containsKey(w)) {
				words.add(w);
			}
		}
		
		return words;
	}
}
