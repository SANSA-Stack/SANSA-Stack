---
title: Composite Transforms
has_children: false
nav_order: 5
---

While it is easy to create individual (static) functions that transform one rdd into another,
chaining such functions directly is cumbersome to use:

```java
# Hard to read because transformations appear in reverse order of their application
JavaRDD<I> input = ...;
JavaRDD<O> output = fn3.apply(fn2.apply(fn1.apply(input)));
```

Chaining allows for a more human readable representation:
```java
RddFunction<I, O> compositeTransform = JavaRddFunction.<I>identity()
    .andThen(fn1)
    .andThen(fn2)
    .andThen(fn3);

JavaRDD<O> output = compositeTransform.apply(input);
```

Unfortunately spark doesn't have a compose function which
would allow more naturally placing the input before the transform:

```java
JavaRDD<Resource> output = input.compose(compositeTransform); # NOT POSSIBLE
```

Spark itself unfortunately does not intrisically provide this mechanism, but SANSA provides a little framework
aimed to easy the creation of such transformation chains.

The interfaces with for chaining are:

* JavaRddFunction
* JavaPairRddFunction
* ToJavaRddFunction
* ToJavaPairRddFunction


## Example

The following example shows an operator that groups Resources and merges their models:

```java
JavaRddFunction<Resource, Resource> compositeTransform =
    JavaRddFunction.<Resource>identity()
        .toPairRdd(JavaRddOfResourcesOps::mapToNamedModels)
        .andThen(rdd -> JavaRddOfNamedModelsOps.groupNamedModels(rdd, true, true, 0))
        .toRdd(JavaRddOfNamedModelsOps::mapToResources);

JavaRDD input = ...;
JavaRDD<Resource> output = compositeTransform.apply(input);
```

An further advantage of such transformation functions is that serialization can be tackled inside of the
transformer:

```java
public static JavaRddFunction<String, String> myTransform(ParamA inputArgA, ParamB inputArgB) {
    return rdd -> {
        # Make the arguments serializable; either using lambda serialization or broadcasts
        String argAStr = unparse(inputArgA);
        Broadcast<ParamB> bc = JavaSparkContext.fromSparkContext(rdd.context()).broadcast(inputArgB);
    
        rdd.mapPartitions(it -> {
            ParamA argA = parse(arg);
            ParamB argB = bc.value();
            return Streams.stream(it).map(createTransformer(argA, argB)).iterator();
        });
    }
}
```



