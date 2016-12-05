# SANSA-Examples on Apache Flink
This is a SANSA-Examples repo for Apache Flink.

## Running the application on a Flink standalone cluster

To run the application on a standalone Flink cluster

1. Setup a Flink cluster
2. Build the application with Maven

  ```
  git clone https://github.com/SANSA-Stack/SANSA-Examples.git
  cd SANSA-Examples/sansa-examples-flink

  mvn clean package

  ```

3. Submit the application to the Flink cluster

  ```
cd /path/to/flink/installation
./bin/flink run -c \
		net.sansa_stack.examples.flink.<SANSA Layer>.<Example> \
 		/app/application.jar \
		FLINK_APPLICATION_ARGUMENTS  
  ```

## Running the application on a Flink standalone cluster via Flink Docker using BDE Platform

To run the SANSA-Examples application on BDE platform, execute the following commands:

```
  git clone https://github.com/SANSA-Stack/SANSA-Examples.git
  cd SANSA-Examples/sansa-examples-flink

  cd config/csswrapper/ && make hosts && cd .. && cd ..

  docker network create hadoop

  docker-compose up -d
```
Note:To make it run, you may need to modify your /etc/hosts file. There is a Makefile, which will do it automatically for you (you should clean up your /etc/hosts after demo).

After BDE platform is up and running, let’s throw some data into our HDFS now by using Hue FileBrowser runing in our network. To perform these actions navigate to 'hue' tab into http://demo.sansa-stack.local. Use “hue” username with any password to login into the FileBrowser (“hue” user is set up as a proxy user for HDFS, see hadoop.env for the configuration parameters). Click on “File Browser” in upper right corner of the screen and use GUI to create /user/root/input and /user/root/output folders and upload the data file into /input folder.
Go to HDFS tab into http://demo.sansa-stack.local and check if the file exists under the path ‘/user/root/input/yourfile’.

After we have all the configuration needed for our example, let’s run our sansa-examples.

```
docker-compose -f sansa-examples.yml up -d --build-arg FLINK_APPLICATION_MAIN_CLASS=net.sansa_stack.examples.flink.rdf.TripleReader 
```

