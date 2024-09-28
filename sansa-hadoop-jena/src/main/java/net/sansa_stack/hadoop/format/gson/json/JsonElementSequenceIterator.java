package net.sansa_stack.hadoop.format.gson.json;

import com.google.common.collect.AbstractIterator;
import com.google.gson.Gson;
import com.google.gson.JsonElement;
import com.google.gson.stream.JsonReader;

import java.io.IOException;

/**
 * Read a sequence of JSON elements without separator. A "natural" separator for SON elements may be newline but
 * this is not required.
 */
public class JsonElementSequenceIterator
        extends AbstractIterator<JsonElement>
        implements AutoCloseable {

    protected Gson gson;
    protected JsonReader reader;

    public JsonElementSequenceIterator(Gson gson, JsonReader reader) {
        this.gson = gson;
        this.reader = reader;
    }

    @Override
    protected JsonElement computeNext() {
        JsonElement result;
        try {
            if (reader.hasNext()) {
                result = gson.fromJson(reader, JsonElement.class);
                // System.out.println("READ: " + item);
            } else {
                result = endOfData();
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        return result;
    }

    @Override
    public void close() {
        if (reader != null) {
            try {
                reader.close();
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }
    }
}
