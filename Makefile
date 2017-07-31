default:
	mkdir -p examples/jars
	wget -O examples/jars/sansa-examples-spark.jar https://github.com/SANSA-Stack/SANSA-Examples/releases/download/develop/sansa-examples-spark_2.11-develop.jar

load-data:
	docker run -it --rm -v $(shell pwd)/examples/data:/data --net spark-net -e "CORE_CONF_fs_defaultFS=hdfs://namenode:8020" bde2020/hadoop-namenode:1.1.0-hadoop2.8-java8 hdfs dfs -copyFromLocal /data /data
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

build-cli:
	docker-compose -f docker-compose-app.yml build

cli-triples-reader: build-cli
	docker-compose -f docker-compose-app.yml up triples-reader

cli-triple-ops: build-cli
	docker-compose -f docker-compose-app.yml up triple-ops

cli-triples-writer: build-cli
	docker-compose -f docker-compose-app.yml up triples-writer

cli-pagerank: build-cli
	docker-compose -f docker-compose-app.yml up pagerank

cli-rdf-stats: build-cli
	docker-compose -f docker-compose-app.yml up rdf-stats

cli-inferencing: build-cli
	docker-compose -f docker-compose-app.yml up inferencing

cli-sparklify: build-cli
	docker-compose -f docker-compose-app.yml up sparklify

cli-owl-reader-manchester: build-cli
	docker-compose -f docker-compose-app.yml up owl-reader-manchester

cli-owl-reader-functional: build-cli
	docker-compose -f docker-compose-app.yml up owl-reader-functional

cli-owl-dataset-reader-manchester: build-cli
	docker-compose -f docker-compose-app.yml up owl-dataset-reader-manchester

cli-owl-dataset-reader-functional: build-cli
	docker-compose -f docker-compose-app.yml up owl-dataset-reader-functional

cli-clustering: build-cli
	docker-compose -f docker-compose-app.yml up clustering

cli-rule-mining: build-cli
	docker-compose -f docker-compose-app.yml up rule-mining
