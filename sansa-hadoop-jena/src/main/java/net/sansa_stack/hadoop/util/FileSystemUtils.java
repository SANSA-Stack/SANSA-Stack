package net.sansa_stack.hadoop.util;

import java.io.IOException;
import java.io.InputStream;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionCodecFactory;

public class FileSystemUtils {
    public static InputStream newInputStream(Path path, Configuration conf) throws IOException {
        // Q: Do we need to close the FileSystem? If so, the InputStream.close() could be wrapped.
        // A: Not totally sure but apparently we can/should rely on hadoop's FileSystem caching
        //    and therefore we do not need to close instances ourselves
        //    See also: https://stackoverflow.com/questions/55168902/should-hadoop-filesystem-be-closed
        FileSystem fs = FileSystem.get(conf);
        return newInputStream(path, fs, conf);
    }

    public static InputStream newInputStream(Path path, FileSystem fs, Configuration conf) throws IOException {
        CompressionCodecFactory compressionCodecFactory = new CompressionCodecFactory(conf);
        return newInputStream(path, fs, compressionCodecFactory);
    }

    public static InputStream newInputStream(Path path, FileSystem fs, CompressionCodecFactory compressionCodecFactory) throws IOException {
        InputStream result = fs.open(path);
        if (compressionCodecFactory != null) {
            CompressionCodec codec = compressionCodecFactory.getCodec(path);
            if (null != codec) {
                result = codec.createInputStream(result);
            }
        }
        return result;
    }
}
