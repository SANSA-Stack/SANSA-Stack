package net.sansa_stack.rdf.spark.utils.kryo.io;

import java.nio.ByteBuffer;

import org.apache.spark.SparkEnv;
import org.apache.spark.serializer.Serializer;

import scala.reflect.ClassTag;

/**
 * Java object serialization using Kryo. This is much more efficient, but Kryo
 * sometimes is buggy to use. We use this mainly to serialize the object
 * inspectors.
 */
public class JavaKryoSerializer {

	public static Serializer getSerializer() {
		Serializer result = SparkEnv.get().serializer();
		return result;
	}

	public static <T> byte[] serialize(ClassTag<T> classTag, T o) {
		Serializer serializer = getSerializer();

		byte[] result = serializer.newInstance().serialize(o, classTag).array();

		//System.out.println("Serialized " + (result == null ? -1 : result.length) + " bytes with class tag " + classTag);

		return result;
	}

	public static <T> T deserialize(ClassTag<T> classTag, byte[] bytes) {
		Serializer serializer = getSerializer();

		//System.out.println("Deserializing " + (bytes == null ? -1 : bytes.length) + " bytes with class tag " + classTag);

		T result = serializer.newInstance().deserialize(ByteBuffer.wrap(bytes), classTag);
		return result;
	}
}