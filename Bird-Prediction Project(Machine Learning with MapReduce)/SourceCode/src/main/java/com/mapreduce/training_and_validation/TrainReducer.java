package com.mapreduce.training_and_validation;

import java.io.IOException;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.Formatter;
import java.util.List;
import java.util.Locale;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;

import de.bwaldvogel.liblinear.Feature;
import de.bwaldvogel.liblinear.FeatureNode;
import de.bwaldvogel.liblinear.Linear;
import de.bwaldvogel.liblinear.Model;
import de.bwaldvogel.liblinear.Parameter;
import de.bwaldvogel.liblinear.Problem;
import de.bwaldvogel.liblinear.SolverType;

/** TrainReducer:
 * - K Bags are created and for records in each of the bags, train k individual models M1, M2,..., Mk, each separately
   - Save each of the models trained by K Reducers i.e. after completion of Training MR Job, K models will be generated and saved 
   
   - TRAINING OF MODEL
     # Use Logistic Regression train method/model
     # Using LIBLINEAR library for training   
 * 
 * */


public class TrainReducer extends Reducer<IntWritable, Text, Text, NullWritable> {
	int numOfTrainingExamples, numOfFeatures = 963; //963;
	private Feature[][] featureNodeValues;
	private double[] targetValues;
	private Model model;
	static final Locale DEFAULT_LOCALE = Locale.ENGLISH;
	static final Charset FILE_CHARSET= Charset.forName("ISO-8859-1");


	public void reduce(IntWritable key, Iterable<Text> values, Context context)
			throws IOException, InterruptedException {
		List<String> data = new ArrayList<>();
		for (Text val : values) {
			data.add(val.toString());
		}

		/* LIBLINEAR TRAINING CONFIGURATION */
		
		numOfTrainingExamples = data.size();
		prepareFeatureNodes(data);

		/* train */
		Problem problem = new Problem();
		problem.l = numOfTrainingExamples; // number of training examples
		problem.n = numOfFeatures; // number of features
		problem.x = featureNodeValues; // feature nodes
		problem.y = targetValues; // target values

		SolverType solver = SolverType.L2R_L1LOSS_SVR_DUAL; // -s 0
		double C = 1.0; // cost of constraints violation
		double eps = 0.01; // stopping criteria

		Parameter parameter = new Parameter(solver, C, eps);
		
		model = Linear.train(problem, parameter); // train the model
		
		try {
			String modelFileContent = saveModel();
			context.write(new Text(modelFileContent), NullWritable.get());
			
		} catch (Exception e) {
			// TODO Auto-generated catch block
			System.out.println("Exception");
			e.printStackTrace();
		}
	}
	
	/** saveModel() is the utility to save the model generated to local file system*/
	
	public String saveModel() throws IOException{
		int nr_feature = model.getNrFeature();
        int w_size = nr_feature;
        if (model.getBias() >= 0) w_size++;

        int nr_w = model.getNrClass();
        	nr_w = 1;
        	StringBuilder sb = new StringBuilder();
        Formatter formatter1 = new Formatter(sb, DEFAULT_LOCALE);
        try {
            
            printf(formatter1, "solver_type %s\n", "L2R_L1LOSS_SVR_DUAL");
            printf(formatter1, "nr_class %d\n", model.getNrClass());

            printf(formatter1, "nr_feature %d\n", nr_feature);
            printf(formatter1, "bias %.16g\n", model.getBias());

            printf(formatter1, "w\n");
            for (int i = 0; i < w_size; i++) {
                for (int j = 0; j < nr_w; j++) {
                    double value = model.getFeatureWeights()[i * nr_w + j];

                    /** this optimization is the reason for {@link Model#equals(double[], double[])} */
                    if (value == 0.0) {
                        printf(formatter1, "%d ", 0);
                    } else {
                        printf(formatter1, "%.16g ", value);
                    }
                }
                printf(formatter1, "\n");
            }

            formatter1.flush();
            IOException ioException = formatter1.ioException();
            if (ioException != null) throw ioException;
        }
        finally {
            formatter1.close();
        }
        
        return sb.toString();
	}
	

	public void printf(Formatter formatter, String format, Object... args) throws IOException {
		
        formatter.format(format, args);
        IOException ioException = formatter.ioException();
        if (ioException != null) throw ioException;
    }
	
	/** prepareFeatureNodes() prepares the data in required structure for liblinear training */
	
	private void prepareFeatureNodes(List<String> data) {

		featureNodeValues = new Feature[numOfTrainingExamples][]; //feature data
		targetValues = new double[numOfTrainingExamples]; // label data
		int index = 0;
		for (String e : data) {
			String[] tokens = e.split(",");

			featureNodeValues[index] = new Feature[tokens.length - 1];
			for (int i = 0, insertIndex = 0; i < tokens.length; i++) {
				
				if (i == 0) {
					String l = tokens[0].split(":")[1];
					Double lable = 1.0;
					if(!l.equalsIgnoreCase("X")){
						lable = Double.parseDouble(l);
					}
					
					targetValues[index] = lable;
				} else {
					
					String[] values =  tokens[i].split(":");
					
					featureNodeValues[index][insertIndex] = new FeatureNode(Integer.parseInt(values[0]),
							Double.parseDouble(values[1]));
					insertIndex++;
				}
			}
			index++;
		}
	}
}