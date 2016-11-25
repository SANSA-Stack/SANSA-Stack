## Not ready yet.
# SANSA-Examples docker demo on Apache Spark
This is a SANSA-Examples docker repo for Apache Spark docker.

## Running the application on a Spark standalone cluster via Spark Docker using BDE Platform

To run the SANSA-Examples application ona BDE platform, execute the following commands:
```
  git clone https://github.com/SANSA-Stack/SANSA-Examples.git
  cd SANSA-Examples

  cd csswrapper/ && make hosts && cd ..

  docker network create hadoop
  docker-compose up -d --build-arg NODE_ENV=production build
```
Note:To make it run, you may need to modify your /etc/hosts file. There is a Makefile, which will do it automatically for you (you should clean up your /etc/hosts after demo).

After BDE platform is up and running, let’s throw some data into our HDFS now by using Hue FileBrowser runing in our network. To perform these actions navigate to 'hue' tab into http://demo.sansa.local. Use “hue” username with any password to login into the FileBrowser (“hue” user is set up as a proxy user for HDFS, see hadoop.env for the configuration parameters). Click on “File Browser” in upper right corner of the screen and use GUI to create /user/root/input and /user/root/output folders and upload the data file into /input folder.
Go to HDFS tab into http://demo.sansa.local and check if the file exists under the path ‘/user/root/input/yourfile’.

After we have all the configuration needed for our example, let’s run our sansa-examples.

```
docker-compose -f sansa-examples.yml up -d --build-arg SPARK_APPLICATION_MAIN_CLASS=net.sansa_stack.examples.spark.rdf.TripleReader 
```

