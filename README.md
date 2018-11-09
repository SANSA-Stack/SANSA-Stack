# SANSA-Examples
[![Build Status](https://ci.aksw.org/jenkins/job/SANSA%20Examples/job/develop/badge/icon)](https://ci.aksw.org/jenkins/job/SANSA%20Examples/job/develop/)
[![License](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](https://opensource.org/licenses/Apache-2.0)
[![Twitter](https://img.shields.io/twitter/follow/SANSA_Stack.svg?style=social)](https://twitter.com/SANSA_Stack)

This directory contains code examples for various SANSA functionality.

### [sansa-examples-spark](https://github.com/SANSA-Stack/SANSA-Examples/tree/master/sansa-examples-spark)
Contains the SANSA Examples for [Apache Spark](http://spark.apache.org/).

### [sansa-examples-flink](https://github.com/SANSA-Stack/SANSA-Examples/tree/master/sansa-examples-flink)
Contains the SANSA Examples for [Apache Flink](http://flink.apache.org/).

## Not able to build the project with maven then read this section:
*Step 1:* just make sure you are using the updated Maven Package Manager. Just run the `mvn --version` to be sure that your mvn is the latest. If it is not then run following commands in your prefered termianl:
As the project is under development might have some problems and end up with errors during the `mvn clean install -U` building process. Therefore, hopefully there is a solution to solve this issue:

```
sudo apt-get --only-upgrade install maven
mvn --versoin
```
if it does not update your current maven (last version is 3.6.0) then follow these commands:

``` 
cd ~/Downloads
wget http://apache.mirror.digionline.de/maven/maven-3/3.6.0/binaries/apache-maven-3.6.0-bin.tar.gz
sudo mkdir -p /usr/local/apache-maven
sudo mv apache-maven-3.6.0-bin.tar.gz /usr/local/apache-maven
cd /usr/local/apache-maven
sudo tar -xzvf apache-maven-3.6.0-bin.tar.gz
```
*Step 2:* then based on your default `$SHELL` add following env variables:

``` 
#MAVEN env variables
export M2_HOME=/usr/local/apache-maven/apache-maven-3.6.0
export M2=$M2_HOME/bin
export MAVEN_OPTS="-Xms256m -Xmx512m"
export PATH=$M2:$PATH
``` 
Do not remember to source new environmental variables:
``` 
source ~/.bashrc
```

Now just check if the installation process is done correctly run this command:

``` 
mvn --version
```

Results: 

![alt text](https://i.imgur.com/gRK41hK.png)

![alt text](https://i.imgur.com/3BwAp7f.png)


