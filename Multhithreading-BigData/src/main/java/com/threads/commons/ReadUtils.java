package com.threads.commons;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;


// This is the loader routine, it will just read the file contents 
// and return an array list of String containing each line as one element 
public class ReadUtils {

	public static List<String> loader(String file){
		
		//File file2 = new File(ReadUtils.class.getClass().getResource("/"+file).toURI());
		List<String> lineList = new ArrayList<>();
		//InputStream in = ReadUtils.class.getClass().getResourceAsStream(file); 
		try (BufferedReader br = new BufferedReader(new FileReader(file))) {

			String line;
			while ((line = br.readLine()) != null) {
				lineList.add(line);
			}

		} catch (IOException e) {
			e.printStackTrace();
		}

		return lineList;
	}
	
	// This is a function to divide the list into number of parts and return sub-lists
	// It takes two arguments: a list and number of parts and chops a list into number of
	// parts
	// Reference: http://stackoverflow.com/questions/2895342
	public static List<List<String>> chopLists( List<String> ls, int parts )
	{
	    List<List<String>> lsParts = new ArrayList<List<String>>();
	    int chunkSize = ls.size() / parts;
	    int leftOver = ls.size() % parts;
	    int take = chunkSize;

	    for( int i = 0, k = ls.size(); i < k; i += take )
	    {
	        if(leftOver > 0 )
	        {
	            leftOver--;
	            take = chunkSize + 1;
	        }
	        else
	        {
	            take = chunkSize;
	        }

	        lsParts.add( new ArrayList<String>( ls.subList( i, Math.min( k, i + take ) ) ) );
	    }

	    return lsParts;
	}
	
	public static void calculateAverageTemprature(HashMap<String,Station> stationMap){
		for(Map.Entry<String,Station> obj:stationMap.entrySet()){
			Station temp = obj.getValue();
			temp.setAverageTMAX(temp.getTmaxTotal()/temp.getRecordCount());
		}
	}
	
}
