
DIR := $(shell basename `pwd`)

$(DIR).jar: *.java build.gradle Makefile
	gradle build
	gradle shadowJar
	cp build/libs/$(DIR)-all.jar $(DIR).jar

run: $(DIR).jar
	zip -d $(DIR).jar META-INF/LICENSE
	zip -d $(DIR).jar LICENSE
	jar tvf $(DIR).jar | grep -i license
	hadoop jar $(DIR).jar data.txt.bz2 output 2>&1| tee hadoop.log

clean:
	rm -rf build $(DIR).jar samples output .gradle

test: run
	perl test.pl | tee test.log

.PHONY: run clean test
