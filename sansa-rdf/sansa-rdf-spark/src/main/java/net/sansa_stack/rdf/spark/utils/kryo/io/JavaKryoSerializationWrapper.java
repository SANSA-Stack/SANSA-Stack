package net.sansa_stack.rdf.spark.utils.kryo.io;

import scala.reflect.ClassTag;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;

/**
 * A wrapper around some unserializable objects that make them both Java
 * serializable. Internally, Kryo is used for serialization.
 *
 * Use KryoSerializationWrapper(value) to create a wrapper.
 */
public class JavaKryoSerializationWrapper<T> implements Serializable {
	private static final long serialVersionUID = 1L;

	protected transient T value;
	protected ClassTag<T> classTag;
	protected byte[] valueSerialized;

	public JavaKryoSerializationWrapper(T value) {
		this.value = value;

		Class<?> clazz = value.getClass();
		this.classTag = scala.reflect.ClassTag$.MODULE$.apply(clazz);
//		System.out.println("ClassTag: " + classTag);

	}

	public byte[] getValueSerialized() {
//		if (valueSerialized == null) {
			valueSerialized = JavaKryoSerializer.serialize(classTag, value);
//		}
		return valueSerialized;
	}

	public void setValueSerialized(byte[] bytes) {
		valueSerialized = bytes;
		value = JavaKryoSerializer.deserialize(classTag, valueSerialized);
	}

	public T getValue() {
		return value;
	}

	public static <I, O> java.util.function.Function<I, O> wrap(java.util.function.Function<I, O> fn) {
		JavaKryoSerializationWrapper<java.util.function.Function<I, O>> wrapper = new JavaKryoSerializationWrapper<>(fn);

		java.util.function.Function<I, O> result = i -> wrapper.getValue().apply(i);

		return result;
	}

	public static <I, O> org.apache.spark.api.java.function.Function<I, O> wrap(org.apache.spark.api.java.function.Function<I, O> fn) {
		JavaKryoSerializationWrapper<org.apache.spark.api.java.function.Function<I, O>> wrapper = new JavaKryoSerializationWrapper<>(fn);

		org.apache.spark.api.java.function.Function<I, O> result = i -> wrapper.getValue().call(i);

		return result;
	}

//	public static <I, O> MapFunction<I, O> wrap(MapFunction<I, O> fn) {
//		JavaKryoSerializationWrapper<MapFunction<I, O>> wrapper = new JavaKryoSerializationWrapper<>(fn);
//
//		MapFunction<I, O> result = i -> wrapper.getValue().call(i);
//
//		return result;
//	}

	// Used for Java serialization.
	private void writeObject(ObjectOutputStream out) throws IOException {
		getValueSerialized();
		out.defaultWriteObject();
	}

	private void readObject(ObjectInputStream in) throws ClassNotFoundException, IOException {
		in.defaultReadObject();
		setValueSerialized(valueSerialized);
	}
}
