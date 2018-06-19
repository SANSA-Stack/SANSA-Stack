###  The distribution does not include the Wordnet dictionary files.
 
### These can be installing as below:

##### 1. Create two folder with name "link" in the root directory of the project
 
##### 2. Make sure 'sbt' is installed on the operating system and if so, execute this command in the command line:
```
sbt download-database
```
##### 3. Delete "./project/target" folder
##### 4. Set your sbt version in "./project/build.properties" to 0.13.9 and repeat step 2