default:
	mkdir -p jars
	wget -O jars/sansa.jar https://github.com/SANSA-Stack/SANSA-Stack/releases/download/v0.8.0-RC1/sansa-stack-spark_2.12-0.8.0-RC1-jar-with-dependencies.jar		
up:
	docker network create spark-net
	docker-compose pull ; docker-compose up -d
down:
	docker-compose down
	docker network rm spark-net

loadSansaToHdfs:
	docker run -it --rm -v $(shell pwd)/jars:/data --net spark-net -e "CORE_CONF_fs_defaultFS=hdfs://namenode:8020" fmoghaddam/sansanamenode:v1 hdfs dfs -copyFromLocal /data /data

updateSansaInHdfs:
	docker run -it --rm -v $(shell pwd)/jars:/data --net spark-net -e "CORE_CONF_fs_defaultFS=hdfs://namenode:8020" fmoghaddam/sansanamenode:v1 hdfs dfs -copyFromLocal -f /data /

