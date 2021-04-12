default:
	mkdir -p examples/jars
	wget -O examples/jars/sansa-examples-spark.jar https://github.com/SANSA-Stack/SANSA-Stack/releases/download/v0.8.0-RC1/sansa-stack-spark_2.12-0.8.0-RC1-jar-with-dependencies.jar

load-data:
	docker run -it --rm -v $(shell pwd)/examples/data:/data --net spark-net -e "CORE_CONF_fs_defaultFS=hdfs://namenode:8020" fmoghaddam/sansanamenode:v1 hdfs dfs -copyFromLocal /data /data
	docker exec -it namenode hdfs dfs -ls /data

up:
	docker network create spark-net
	docker-compose up -d

down:
	docker-compose down
	docker network rm spark-net

restart:
	docker-compose stop zeppelin
	docker-compose rm zeppelin
	docker-compose up -d zeppelin
