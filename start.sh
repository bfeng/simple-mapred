java -cp target/simple-mapred-1.0-SNAPSHOT-jar-with-dependencies.jar io.github.bfeng.simplemapred.resource.Master src/main/resources/machines.conf


java -cp target/simple-mapred-1.0-SNAPSHOT-jar-with-dependencies.jar io.github.bfeng.simplemapred.resource.Worker src/main/resources/machines.conf 0


java -cp target/simple-mapred-1.0-SNAPSHOT-jar-with-dependencies.jar io.github.bfeng.simplemapred.resource.Worker src/main/resources/machines.conf 1


java -cp target/simple-mapred-1.0-SNAPSHOT-jar-with-dependencies.jar io.github.bfeng.simplemapred.app.WordCountApp
