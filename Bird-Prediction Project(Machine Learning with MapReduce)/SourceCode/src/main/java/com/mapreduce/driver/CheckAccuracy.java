package com.mapreduce.driver;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;

public class CheckAccuracy {

	public static void main(String args[]){

		Integer total = 0;
		Integer correct = 0;
		Double tuneParam =0.48;
		BufferedReader reader = null;
		String line = "";

		for(int i=0;i<3;i++){
			try {
				reader = new BufferedReader(new FileReader("/Users/jain.san/Documents/workspace/BirdSightingPredictionProjectMR/log-test-output/part-m-0000"+i));
				while ((line = reader.readLine()) != null) {
					String details[] = line.split("\t");


					Integer val =Integer.parseInt(details[0]);

					Double prediction = Double.parseDouble(details[1]);

					if(val == 1 && prediction >= tuneParam)
						correct++;

					else if(val == 0 && prediction  < tuneParam)
						correct++;
					total++;
				}
				reader.close();

			} catch (FileNotFoundException e) {
				e.printStackTrace();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}

		Double avg = Double.valueOf((correct*1.0)/total);
		System.out.println("Accuracy Percentage "+ avg*100);


	}
}
