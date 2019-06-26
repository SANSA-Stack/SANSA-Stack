
* Forcing a parent version on the layers can be done with the following maven invocation. The parent version **must** be *resolvable*.

```bash
mvn -N versions:update-parent -DparentVersion='[0.6.0-SNAPSHOT]' -DallowSnapshots=true versions:update-child-modules
```

