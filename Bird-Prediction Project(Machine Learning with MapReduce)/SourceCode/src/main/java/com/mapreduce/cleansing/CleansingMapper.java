package com.mapreduce.cleansing;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;


/** CleansingMapper 
 *  a) extracts only the relevant feature data that are of importance from checklist, core, and extended features
 *  b) cleans the selected records i.e. handling of '?' and 'X' for each of the features ,
 *  c) emits data extracted in sparse data with key as location id 
 * */

public class CleansingMapper extends Mapper<Object, Text, Text, Text>{
	
    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
    	
    	if(value.toString().contains("YEAR"))
    		return;
    	
    	String record = value.toString();
    	
    	
    	String sampling_event_id = record.substring(0,record.indexOf(","));
    	
    	// splitting on sampling_id  will seperate data into checklist, core and extended
    	// from each of these categories, extract only the features of interest
    	String []files = record.split(sampling_event_id);
    	
    	String checklist = files[1];
    	String core = files[2];
    	String extended = files[3];
    	
    	// extract and format checklist data in sparse format
    	String checklistFormatted = getChecklist(checklist);
    	
    	// extract and format core data in sparse format
    	String coreFormatted = getCore(core);
    	
    	// extract and format extended data in sparse format
    	String extendedFormatted = getExtended(extended);
    	
    	if(checklistFormatted.trim().isEmpty())
    		return;
    	
    	// combine the extracted sparse representation data
    	String output = checklistFormatted + coreFormatted+ extendedFormatted;
    	
    	String locationId = checklist.split(",")[1];
    	
    	// emit the record with location id as key
    	if(!locationId.trim().isEmpty())
    		context.write(new Text(locationId), new Text(output));
    }
    
    /** getExtended() is a helper method to extract, 
     * clean extended features and represent in sparse format
     * */
    
    private String getExtended(String extended) {
    	String []extendedColums = extended.split(",");
    	
    	String distFromFlowingWater = extendedColums[74];
    	String distInFlowingWater = extendedColums[75];
    	String distFromStandingFresh = extendedColums[76];
    	String distInStandingFresh = extendedColums[77];
    	String distFromWetVeg = extendedColums[78];
    	String distInWetVeg = extendedColums[79];
    	
    	
    	StringBuilder sb = new StringBuilder();
    	
    	if(!distFromFlowingWater.trim().isEmpty() && !distFromFlowingWater.contains("?") && !distFromFlowingWater.contains("X")){
    		sb.append(",958:"+ distFromFlowingWater);
    	}
    	
    	if(!distInFlowingWater.trim().isEmpty() && !distInFlowingWater.contains("?") && !distInFlowingWater.contains("X")){
    		sb.append(",959:"+ distInFlowingWater);
    	}
    	
    	if(!distFromStandingFresh.trim().isEmpty() &&!distFromStandingFresh.contains("?") && !distFromStandingFresh.contains("X")){
    		sb.append(",960:"+ distFromStandingFresh);
    	}
    	
    	if(!distInStandingFresh.trim().isEmpty() &&!distInStandingFresh.contains("?") && !distInStandingFresh.contains("X")){
    		sb.append(",961:"+ distInStandingFresh);
    	}
    	
    	if(!distFromWetVeg.trim().isEmpty() &&!distFromWetVeg.contains("?") && !distFromWetVeg.contains("X")){
    		sb.append(",962:"+ distFromWetVeg);
    	}
    	
    	if(!distInWetVeg.trim().isEmpty() && !distInWetVeg.contains("?") && !distInWetVeg.contains("X")){
    		sb.append(",963:"+ distInWetVeg);
    	}
    	
		return sb.toString().trim();
	}


    /** getCore() is a helper method to extract, 
     * clean core features and represent in sparse format
     * */
    
	private String getCore(String core) {
String []coreColums = core.split(",");
    	
    	String elev_ned = coreColums[6];
    	String bcr = coreColums[7];
    	String avgTemp = coreColums[10];
    	String precipitation = coreColums[13];
    	String snow = coreColums[14];
    	
    	
    	StringBuilder sb = new StringBuilder();
    	
    	if(!elev_ned.trim().isEmpty() && !elev_ned.contains("?") && !elev_ned.contains("X")){
    		sb.append(",953:"+ elev_ned);
    	}
    	
    	if(!bcr.trim().isEmpty() && !bcr.contains("?") && !bcr.contains("X")){
    		sb.append(",954:"+ bcr);
    	}
    	
    	if(!avgTemp.trim().isEmpty() && !avgTemp.contains("?") && !avgTemp.contains("X")){
    		sb.append(",955:"+ avgTemp);
    	}
    	
    	if(!precipitation.trim().isEmpty() && !precipitation.contains("?") && !precipitation.contains("X")){
    		sb.append(",956:"+ precipitation);
    	}
    	
    	if(!snow.trim().isEmpty() && !snow.contains("?") && !snow.contains("X")){
    		sb.append(",957:"+ snow);
    	}
    	
		return sb.toString().trim();
	}


	
    /** getChecklist() is a helper method to extract, 
     * clean checklist features and represent in sparse format
     * */
	public String getChecklist(String checklist){
    	String []checklistColums = checklist.split(",");
		
    	String locationId = checklistColums[1];
    	
		String year = checklistColums[4];
		
		if(year.equals("?")) return "";
		
		Long yearVal = Long.parseLong(year);
		String month = checklistColums[5];
		String day = checklistColums[6];
		String time = checklistColums[7];
		
		if(month.equals("?") || day.equals("?"))  return "";
		
		Integer monthVal = Integer.parseInt(month);
		Integer dayVal = Integer.parseInt(day);
		Double timeVal = Double.parseDouble(time);
		
		
		String count_type = checklistColums[11];
		Integer countTypeVal = 7;
		
		if(!count_type.equals("?")){
			countTypeVal = Integer.parseInt(""+count_type.charAt(2))+1;
			if(countTypeVal > 6)
				countTypeVal = 7;
		}


		String effort_hrs = checklistColums[12];
		String effort_dist = checklistColums[13];
		String num_of_observers = checklistColums[16];

		if(effort_hrs.equals("?"))
			effort_hrs = "0.5";
		
		if(effort_dist.equals("?"))
			effort_dist = "0";
		
		if(num_of_observers.equals("?"))
			num_of_observers = "1";
		
		String agelaius_phoeniceus = checklistColums[26];
		
		Integer agelaius_phoeniceusVal = 0;
		
		if(agelaius_phoeniceus.equals("?"))
			agelaius_phoeniceusVal = 0;
		else if(agelaius_phoeniceus.equalsIgnoreCase("X"))
			agelaius_phoeniceusVal = 1;
		else if(!agelaius_phoeniceus.equals("0"))
			agelaius_phoeniceusVal = 1;
		
			
		String speciesString = "";
		
		for(int i=19;i<=952;i++){
			if(i == 26){
				continue;
			}
			
			String specie = checklistColums[i];
			Integer specieVal = 0;
			
			if(specie.equals("?"))
				specieVal = 0;
			else if(specie.equalsIgnoreCase("X"))
				specieVal = 1;
			else if(!specie.equals("0"))
				specieVal = 1;
			
			if(specieVal > 0)
				speciesString = speciesString + i+":"+1+ ",";
		}
		
		if(speciesString.length() > 2)
		speciesString = speciesString.substring(0, speciesString.length()-1);
		
		
		StringBuilder sb = new StringBuilder();
		
		sb.append("0:"+agelaius_phoeniceusVal);
		
		
		if(yearVal > 0){
			sb.append(",2:"+yearVal);
		}
		
		if(monthVal > 0){
			sb.append(",3:"+monthVal);
		}
		
		if(dayVal > 0){
			sb.append(",4:"+dayVal);
		}
		
		if(timeVal > 0){
			sb.append(",5:"+timeVal);
		}
		
		if(countTypeVal > 0){
			sb.append(",6:"+countTypeVal);
		}
		if(Double.parseDouble(effort_hrs) > 0.0){
			sb.append(",7:"+effort_hrs);
		}
		if(Double.parseDouble(effort_dist) > 0.0){
			sb.append(",8:"+effort_dist);
		}
		if(Double.parseDouble(num_of_observers) > 0.0){
			sb.append(",9:"+num_of_observers);
		}
		
		if(!speciesString.trim().isEmpty()){
			sb.append(",");
			sb.append(speciesString);
		}
		
		return sb.toString();
		
    }
    
  }
