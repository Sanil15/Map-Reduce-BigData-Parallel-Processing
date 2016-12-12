package com.mapreduce.prediction;

import java.io.BufferedInputStream;
import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.InputStreamReader;
import java.util.HashMap;
import java.util.Map;

import org.apache.commons.compress.compressors.CompressorInputStream;
import org.apache.commons.compress.compressors.CompressorStreamFactory;


/** OrderingSamplingId performs ordering of records based on SAMPLING ID,
 *  in the order of appearance in UNLABLED DATA*/

public class OrderingSamplingId {

	public static void runOrder(String predFileDir, String unlabledFile, String output) {

		Map<String, Integer> predictions = new HashMap<>();
		String unlabledPredFileDir = predFileDir;
		String unlabeledDataFile = unlabledFile;
		
		try{
		
			BufferedReader br = null;
			String line = "";
			File dir = new File(predFileDir);
			for( File file : dir.listFiles()){
				if(file.getName().contains("part-r") && !file.getName().contains("crc") && !file.getName().contains("SUCCESS")){
					br = new BufferedReader(new FileReader(file));
					
					while((line = br.readLine()) != null){
						if(line.trim().isEmpty()) continue;
						String[] tokens = line.split("\t");
						if(tokens.length!=2) continue;
						predictions.put(tokens[0], Integer.parseInt(tokens[1]));
					}
					br.close();	
				}
			}
			
			
			
			StringBuilder sb = new StringBuilder();
			File unlabledDir = new File(unlabeledDataFile);
			for(File f : unlabledDir.listFiles()){
				FileInputStream fin = new FileInputStream(f);
			    BufferedInputStream bis = new BufferedInputStream(fin);
			    CompressorInputStream input = new CompressorStreamFactory().createCompressorInputStream(bis);
			    br = new BufferedReader(new InputStreamReader(input));

			    int first = 0;
				line = "";
				
				sb.append("SAMPLING_EVENT_ID, SAW_AGELAIUS_PHOENICEUS");
				while((line = br.readLine()) != null){
					if(first == 0){
						first++;
						continue;
					}
					sb.append(System.lineSeparator());
					String samplingId = line.substring(0, line.indexOf(",")).trim();
					sb.append(samplingId);
					sb.append(",");
					sb.append(predictions.get(samplingId));
				}
				br.close();
			}
			
			
			
			
			File file = new File(output);

            // if file doesnt exists, then create it
            if (!file.exists()) {
                file.createNewFile();
            }

            FileWriter fw = new FileWriter(output);
            BufferedWriter bw = new BufferedWriter(fw);
            // write in file
            bw.write(sb.toString());
            // close connection
            bw.close();
		}catch(Exception e){
			e.printStackTrace();
		}
		
	}

}
