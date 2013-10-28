ConfigFile = configuration/config.txt

all:	
	javac -cp .:lib/http-20070405.jar dfs/*.java examples/*.java io/*.java mapreduce/*.java \
	node/*.java socket/*.java util/*.java
	
master: all
	java -cp .:lib/http-20070405.jar node.Master ${ConfigFile}

slave: all
	java -cp .:lib/http-20070405.jar node.SlaveMainRoutine ${ConfigFile}

clean:
	find . -name *.class | xargs rm -rf >> a.out
	find -name *.class | xargs rm -rf >> a.out
	rm a.out
