default: run

run:
	mvn clean install
	java -jar target/BirdSightingPredictionProjectMR-0.0.1-SNAPSHOT-jar-with-dependencies.jar input-labeled input-unlabeled output prediction-unordered prediction-final/Prediction.txt

clean:
	rm -rf prediction-final/*
	rm -rf output/*
	rm -r prediction-unordered

