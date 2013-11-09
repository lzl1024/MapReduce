ConfigFile = configuration/config.txt
InputPath1 = harrypotter.txt
OutputPath1 = harrypotter.out

InputPath2 = story1.txt
OutputPath2 = story1.out
ConfigFile2 = configuration/config_rep1.txt

all:	
	javac -cp .:lib/http-20070405.jar dfs/*.java examples/*.java io/*.java mapreduce/*.java \
	node/*.java socket/*.java util/*.java
	
master: all
	java -cp .:lib/http-20070405.jar node.MasterMain ${ConfigFile}

slave: all
	java -cp .:lib/http-20070405.jar node.SlaveMain ${ConfigFile}

exp1:  all 
	java examples.Exp1WordCountMain ${ConfigFile} ${InputPath1} ${OutputPath1}

exp2:  all 
	java examples.Exp2DFS ${ConfigFile} ${InputPath1} ${OutputPath1}

exp3:  all 
	java examples.Exp3DFSfail ${ConfigFile} ${InputPath2} ${OutputPath2}

clean:
	find . -name *.class | xargs rm -rf >> a.out
	find -name *.class | xargs rm -rf >> a.out
	rm a.out
