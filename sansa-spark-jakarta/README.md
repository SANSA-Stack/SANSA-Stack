## sansa-spark-jakarta

Use the profile `jakarta` to perform shading from `javax` to `jakarta`.

`mvn -Pjakarta package`

This module is a temporary fix and will become obsolete once spark moves to the jakarta packages.

This module builds a shaded version of the artifact `org.apache.spark:spark-sql_2.12` based on jakarta servlet api 5.0.0.
Exclude `org.apache.spark:spark-sql_2.12` when including this artifact.

```xml
<dependency>
        <groupId>net.sansa-stack</groupId>
        <artifactId>sansa-spark-jakarta_2.12</artifactId>
        <version>${sansa.version}</version>
        <classifier>jakarta</classifier>
</dependency>

<dependency>
	<groupId>org.apache.spark</groupId>
	<artifactId>spark-sql_2.12</artifactId>
	<scope>provided</scope>
</dependency>
```

