
* Forcing a parent version on the layers can be done with the following maven invocation. The parent version **must** be *resolvable*.

```bash
mvn -N versions:update-parent -DparentVersion='[0.6.0-SNAPSHOT]' -DallowSnapshots=true versions:update-child-modules
```

### Change Scala version
Prerequisites: We assume that all SANSA projects supposed to be changed are located in the same parent directory 

Run script from within the current project directory and provide the Scala version, i.e. either 2.11 or 2.12
Example to change to Scala 2.12
```bash
./change-scala-version.sh 2.12
```
It will modify all POM files from the SANSA projects and change the artitfact ID, i.e. assuming the current layout

`sansa-LAYERNAME-MODULENAME_SCALAVERSION`

the suffix `_SCALAVERSION` will be modified.
Moreover, we also replace the `scala.binary.version` in the POM file of the SANSA-Parent module.

Note, currently the layers to process is hard coded in https://github.com/SANSA-Stack/SANSA-Parent/blob/scala-2.12/change-scala-version.sh#L51 , `order=(parent rdf ...)`, thus for additional projects this line has to be adapted, otherwise they will simply be ignored by the script.
