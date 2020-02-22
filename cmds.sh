java -cp target/simple-mapred-1.0-SNAPSHOT-jar-with-dependencies.jar io.github.bfeng.simplemapred.resource.Master src/main/resources/machines.conf

java -cp target/simple-mapred-1.0-SNAPSHOT-jar-with-dependencies.jar io.github.bfeng.simplemapred.resource.Worker src/main/resources/machines.conf 0

java -cp target/simple-mapred-1.0-SNAPSHOT-jar-with-dependencies.jar io.github.bfeng.simplemapred.resource.Worker src/main/resources/machines.conf 1

java -cp target/simple-mapred-1.0-SNAPSHOT-jar-with-dependencies.jar io.github.bfeng.simplemapred.app.WordCountApp localhost:12345 input/words-1.txt,input/words-2.txt output/wc-1.txt,output/wc-2.txt

java -cp target/simple-mapred-1.0-SNAPSHOT-jar-with-dependencies.jar io.github.bfeng.simplemapred.app.InvertedIndex localhost:12345 input/words-1.txt,input/words-2.txt output/ii-1.txt,output/ii-2.txt
