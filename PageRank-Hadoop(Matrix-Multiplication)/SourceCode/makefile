default: run

run:
	mvn clean install
	
local1:
	rm -rf pagerank/*
	rm -rf DUMMY/*
	java -jar target/PageRank-0.0.1-SNAPSHOT.jar input preprocessing top100 DUMMY pagerank ROW

local2:
	rm -rf pagerank/*
	rm -rf DUMMY/*
	java -jar target/PageRank-0.0.1-SNAPSHOT.jar input preprocessing top100 DUMMY pagerank COLUMN

clean:
	rm -rf pagerank/*
	rm -rf DUMMY/*
	rm -rf top100
	rm -rf preprocessing