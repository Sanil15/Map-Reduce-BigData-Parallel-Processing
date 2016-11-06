#!/bin/bash
# Author: Nat Tuck

jar:
	sbt package
	cp target/scala-*/demo_*.jar Demo.jar

run: jar
	rm -rf out
	sbt run

prof: jar
	rm -rf out
	SBT_OPTS="-Xprof" sbt run

hadoop: jar
	rm -rf out
	HADOOP_CLASSPATH=`perl cp.pl` hadoop jar Demo.jar

clean:
	rm -rf project target *.jar out
