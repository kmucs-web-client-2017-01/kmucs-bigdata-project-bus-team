package kr.kookmin.cs.bigdata.kkp;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;


public class Preprocessing {
	
	private static final String[] PREWORDLIST = {
		"A", "a", "The", "the", "An", "an",
		"is", "Is", "Are", "are", "am", "Am",
		"and", "And", "or", "Or", "to", "To",
		"for", "For", "of", "Of", "on", "On",
		"in", "In", "with", "With", "this", "This",
		"that", "That", "what", "What", "who", "Who",
		"how", "How", "why", "Why", "where", "Where",
		"when", "When"
	};

	private static final HashMap<String, Integer> PREWORDMAP = new HashMap<String, Integer>(){{
		for(String w : PREWORDLIST) {
			put(w, 1); 
		}
    }};
	
	public static ArrayList<String> removeNeedlessWords(String text)
	{
		Stemmer stemmer = new Stemmer();
		ArrayList<String> words = new ArrayList<String>();
		text = text.replaceAll("[^a-z A-Z 0-9\\s]", " ");
		String[] splitText = text.split(" ");
		
		for(String w : splitText) {
			if(!PREWORDMAP.containsKey(w)) {
				stemmer.add(w.toCharArray(), w.length());
				stemmer.stem();
				String stemmerResult = stemmer.toString();
				if(stemmerResult != " " && stemmerResult.length() > 0)
					words.add(stemmerResult);
			}
		}
		return words;
	}
}
