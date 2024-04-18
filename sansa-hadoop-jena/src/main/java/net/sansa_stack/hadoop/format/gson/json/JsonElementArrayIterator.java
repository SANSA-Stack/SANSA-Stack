package net.sansa_stack.hadoop.format.gson.json;

import com.google.common.collect.AbstractIterator;
import com.google.gson.Gson;
import com.google.gson.JsonElement;
import com.google.gson.stream.JsonReader;
import com.google.gson.stream.JsonToken;

import java.io.EOFException;
import java.io.IOException;

public class JsonElementArrayIterator
        extends AbstractIterator<JsonElement>
        implements AutoCloseable {

    protected Gson gson;
    protected JsonReader reader;
    protected boolean isFirstElt;

    public JsonElementArrayIterator(Gson gson, JsonReader reader) {
        this.gson = gson;
        this.reader = reader;
        this.isFirstElt = false;
    }

    @Override
    protected JsonElement computeNext() {
        JsonElement result;
        try {

            if (!isFirstElt) {
                isFirstElt = true;
                try {
                    reader.beginArray();
                } catch (EOFException x) {
                    result = endOfData();
                    return result;
                }
            }

            if (reader.hasNext()) {
                JsonElement item = gson.fromJson(reader, JsonElement.class);
                result = item;
                // System.out.println("READ: " + item);
            } else {
                reader.endArray();

                // hasNext() seems to return true if there is another token
                // However it may be JsonToken.END_DOCUMENT which actually means there is no
                // more actual data
                boolean hasMore;
                try {
                    hasMore = reader.hasNext();
                } catch (Exception y) {
                    // System.out.println("WARN: " + y);

                    // hasNext() may raise MalformedJsonException
                    // this means that there is more data even though we cannot parse it
                    hasMore = true;
                }

                if (hasMore) {
                    JsonElement nextItem = null;

                    JsonToken token = reader.peek();
                    if (token.equals(JsonToken.END_DOCUMENT)) {
                        result = endOfData();
                    } else {
                        // XXX This was the old logic to check for END_DOCUMENT.
                        // No longer works because gson.fromJson throws a different exception by now
                        // So this snippet can probably be removed
                        try {
                            nextItem = gson.fromJson(reader, JsonElement.class);
                        } catch (IllegalArgumentException y) {
                            // System.out.println("COMPLETED!");
                            result = endOfData();
                        }
                    }

                    if (nextItem != null) {
                        throw new RuntimeException("More items found after array end: " + nextItem);
                    }
                    result = null; // TODO We should probably never come here but the logic is confusing
                } else {
                    // System.out.println("COMPLETED!");
                    result = endOfData();
                }
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
