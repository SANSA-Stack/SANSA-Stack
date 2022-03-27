jarOnly:
	- mvn11 -T 10 clean install -Dmaven.javadoc.skip=true -DskipTests -Dskip -Pdist -U
	- cd sansa-ml/sansa-ml-spark; mvn clean install -Dmaven.javadoc.skip=true -DskipTests -Dskip -Pdist -am -pl :sansa-ml-spark_2.12 -U
fastJar:
	- cd sansa-ml/sansa-ml-spark; mvn clean install -Dmaven.javadoc.skip=true -DskipTests -Dskip -Pdist -am -pl :sansa-ml-spark_2.12 -U

uploadJarOnly:
	- echo 'put ./sansa-ml/sansa-ml-spark/target/sansa-ml-spark_2.12-0.8.0-RC2-SNAPSHOT-jar-with-dependencies.jar .' | sftp FarshadBakhshadneganMoghaddam@akswnc4.aksw.uni-leipzig.de

jarAndUploadAksw4:
	- mvn -T 10 clean install -Dmaven.javadoc.skip=true -DskipTests -Dskip -Pdist -U
	- cd sansa-ml/sansa-ml-spark; mvn clean install -Dmaven.javadoc.skip=true -DskipTests -Dskip -Pdist -am -pl :sansa-ml-spark_2.12 -U
	- echo 'put ./sansa-ml/sansa-ml-spark/target/sansa-ml-spark_2.12-0.8.0-RC2-SNAPSHOT-jar-with-dependencies.jar .' | sftp FarshadBakhshadneganMoghaddam@akswnc4.aksw.uni-leipzig.de

uploadFileHdfsAksw5:
	- echo 'put $(file) .' | sftp FarshadBakhshadneganMoghaddam@akswnc5.aksw.uni-leipzig.de
	- echo 'hadoop fs -put /data/home/FarshadBakhshadneganMoghaddam/$(file) /FarshadBakhshandeganMoghaddam/' | ssh FarshadBakhshadneganMoghaddam@akswnc5.aksw.uni-leipzig.de

runSparkAksw4:
	- echo 'screen -dm bash -c "$(command)"' | ssh FarshadBakhshadneganMoghaddam@akswnc4.aksw.uni-leipzig.de -L 3038:localhost:3038 -L 4040:localhost:4040
