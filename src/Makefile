ConfigFile = configuration/config.txt
#ConfigFile = configuration/config_largeFile.txt

InputPath1 = harrypotter.txt
OutputPath1 = harrypotter.out

InputPath2 = story1.txt
OutputPath2 = story1.out

InputPath3 = pagecounts
OutputPath3 = pagecounts.out

all:	
	javac dfs/*.java examples/*.java io/*.java mapreduce/*.java \
	node/*.java socket/*.java util/*.java
	
master: all
	java node.MasterMain ${ConfigFile}

slave: all
	java node.SlaveMain ${ConfigFile}

exp1:  all 
	java examples.Exp1WordCountMain ${ConfigFile} ${InputPath1} ${OutputPath1}

exp2:  all 
	java examples.Exp2DFS ${ConfigFile} ${InputPath1} ${OutputPath1}

exp3:  all 
	java examples.Exp3DFSfail ${ConfigFile} ${InputPath2} ${OutputPath2}

exp4:  all 
	java examples.Exp4PageRankMain ${ConfigFile} ${InputPath3} ${OutputPath3}

clean:
	find . -name *.class | xargs rm -rf >> a.out
	find -name *.class | xargs rm -rf >> a.out
	rm a.out
