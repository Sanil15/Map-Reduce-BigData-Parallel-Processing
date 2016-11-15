
run:
	rm -rf output
	sbt run

clean:
	rm -rf project target output _test

gen-data:
	mkdir _test
	perl gen-sparse.pl 1000 | sort > _test/dataA.csv
	perl gen-ident.pl 1000  | sort > _test/dataB.csv

test:
	make clean
	make gen-data
	sbt run
	cat output/* | sort > _test/dataC.csv
	perl check.pl _test/dataA.csv _test/dataC.csv

.PHONY: run clean test gen-data
