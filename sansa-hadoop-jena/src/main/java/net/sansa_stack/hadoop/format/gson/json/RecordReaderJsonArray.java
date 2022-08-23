package net.sansa_stack.hadoop.format.gson.json;

import com.google.common.collect.AbstractIterator;
import com.google.common.collect.Streams;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonElement;
import com.google.gson.stream.JsonReader;
import io.reactivex.rxjava3.core.Flowable;
import net.sansa_stack.hadoop.core.Accumulating;
import net.sansa_stack.hadoop.core.RecordReaderGenericBase;
import net.sansa_stack.hadoop.core.pattern.CustomPattern;
import net.sansa_stack.hadoop.core.pattern.CustomPatternJava;
import net.sansa_stack.hadoop.format.jena.base.RecordReaderConf;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import java.io.*;
import java.util.AbstractMap;
import java.util.concurrent.Callable;
import java.util.regex.Pattern;
import java.util.stream.Stream;

public class RecordReaderJsonArray
    extends RecordReaderGenericBase<JsonElement, JsonElement, JsonElement, JsonElement>
{
    public static final String RECORD_MINLENGTH_KEY = "mapreduce.input.json.record.minlength";
    public static final String RECORD_MAXLENGTH_KEY = "mapreduce.input.json.record.maxlength";
    public static final String RECORD_PROBECOUNT_KEY = "mapreduce.input.json.record.probecount";

    // Search for open bracket or comma
    protected static final CustomPattern jsonFwdPattern = CustomPatternJava
            .compile("\\[|,", Pattern.CASE_INSENSITIVE | Pattern.MULTILINE);

    protected Gson gson;

    public RecordReaderJsonArray() {
        this(new GsonBuilder().setLenient().create());
    }

    public RecordReaderJsonArray(Gson gson) {
        this(new RecordReaderConf(
                RECORD_MINLENGTH_KEY,
                RECORD_MAXLENGTH_KEY,
                RECORD_PROBECOUNT_KEY,
                jsonFwdPattern),
                gson);
    }

    public RecordReaderJsonArray(RecordReaderConf conf, Gson gson) {
        super(conf, Accumulating.identity());
        this.gson = gson;
    }

    @Override
    public void initialize(InputSplit inputSplit, TaskAttemptContext context) throws IOException {
        super.initialize(inputSplit, context);
        postambleBytes = new byte[] {']'};
    }

    /**
     * Always replace the first character (which is either
     * a comma or open bracket) with an open bracket in order to
     * mimick a JSON array start.
     *
     * @param base The base input stream
     * @return
     */
    @Override
    protected InputStream effectiveInputStream(InputStream base) {
        PushbackInputStream result = new PushbackInputStream(base);
        try {
            int c = result.read();
            if (c != -1) {
                result.unread('[');
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

        return result;
    }


    public static class JsonElementIterator
                extends AbstractIterator<JsonElement>
                implements AutoCloseable {

        public JsonElementIterator(Gson gson, JsonReader reader) {
            this.gson = gson;
            this.reader = reader;
            this.isFirstElt = false;
        }

        protected Gson gson;

            protected JsonReader reader;
            protected boolean isFirstElt;



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
                            try {
                                nextItem = gson.fromJson(reader, JsonElement.class);
                            } catch (IllegalArgumentException y) {
                                // System.out.println("COMPLETED!");
                                result = endOfData();
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

    protected Stream<JsonElement> parse(InputStream in, boolean isProbe) {
        JsonElementIterator it = new JsonElementIterator(gson, gson.newJsonReader(new InputStreamReader(in)));
        return Streams.stream(it).onClose(it::close);
    }

    // @Override
    protected Flowable<JsonElement> parse(Callable<InputStream> inputStreamSupplier) {
        return Flowable.generate( // <JsonElement, Map.Entry<JsonReader, Boolean>>
                () -> {
                    return new AbstractMap.SimpleEntry<JsonReader, Boolean>(
                            gson.newJsonReader(new InputStreamReader(inputStreamSupplier.call())),
                            false);
                }, // Flag for the first element
                (s, e) -> {
                    try {
                        JsonReader reader = s.getKey();
                        if (!s.getValue()) {
                            s.setValue(true);
                            try {
                                reader.beginArray();
                            } catch (EOFException x) {
                                e.onComplete();
                                return;
                            }
                        }

                        if (reader.hasNext()) {
                            JsonElement item = gson.fromJson(reader, JsonElement.class);
                            // System.out.println("READ: " + item);
                            e.onNext(item);
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
                                try {
                                    nextItem = gson.fromJson(reader, JsonElement.class);
                                } catch (IllegalArgumentException y) {
                                    // System.out.println("COMPLETED!");
                                    e.onComplete();
                                }

                                if (nextItem != null) {
                                    throw new RuntimeException("More items found after array end: " + nextItem);
                                }
                            } else {
                                // System.out.println("COMPLETED!");
                                e.onComplete();
                            }
                        }
                    } catch (Exception x) {
                        e.onError(x);
                    }
                },
                s -> s.getKey().close());
    }
}
