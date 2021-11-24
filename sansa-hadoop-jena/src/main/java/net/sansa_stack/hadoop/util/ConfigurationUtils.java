package net.sansa_stack.hadoop.util;

import com.google.common.io.BaseEncoding;
import org.apache.commons.lang3.SerializationUtils;
import org.apache.hadoop.conf.Configuration;

import java.io.Serializable;

public class ConfigurationUtils {
    /** Get a (non-null) string as a base64 url encoded serialized object */
    public static <T> T getSerializable(Configuration conf, String key, T defaultValue) {
        String str = conf.get(key);
        T result = defaultValue;
        if (str != null) {
            byte[] data = BaseEncoding.base64Url().omitPadding().decode(str);
            Object obj = SerializationUtils.deserialize(data);
            result = (T)obj;
        }
        return result;
    }

    /** Set a serializable object as a base64 url encoded string */
    public static void setSerializable(Configuration conf, String key, Serializable serializable) {
        byte[] data = SerializationUtils.serialize(serializable);
        String str = BaseEncoding.base64Url().omitPadding().encode(data);
        conf.set(key, str);
    }
}
