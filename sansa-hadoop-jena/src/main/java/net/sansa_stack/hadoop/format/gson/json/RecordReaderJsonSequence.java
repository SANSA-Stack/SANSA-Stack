package net.sansa_stack.hadoop.format.gson.json;

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

public class RecordReaderJsonSequence
    extends RecordReaderGenericBase<JsonElement, JsonElement, JsonElement, JsonElement>
{
    public static final String RECORD_MINLENGTH_KEY = "mapreduce.input.json.record.minlength";
    public static final String RECORD_MAXLENGTH_KEY = "mapreduce.input.json.record.maxlength";
    public static final String RECORD_PROBECOUNT_KEY = "mapreduce.input.json.record.probecount";

    // Search for open brace, bracket, quote or numbers starting with digit or dot
    protected static final CustomPattern jsonFwdPattern = CustomPatternJava
            .compile("\\{|\\[|\"|\\d|\\.", Pattern.CASE_INSENSITIVE | Pattern.MULTILINE);

    protected Gson gson;

    public RecordReaderJsonSequence() {
        this(new GsonBuilder().setLenient().create());
    }

    public RecordReaderJsonSequence(Gson gson) {
        this(new RecordReaderConf(
                null,
                RECORD_MINLENGTH_KEY,
                RECORD_MAXLENGTH_KEY,
                RECORD_PROBECOUNT_KEY,
                jsonFwdPattern),
                gson);
    }

    public RecordReaderJsonSequence(RecordReaderConf conf, Gson gson) {
        super(conf, Accumulating.identity());
        this.gson = gson;
    }

    @Override
    public void initialize(InputSplit inputSplit, TaskAttemptContext context) throws IOException {
        super.initialize(inputSplit, context);
    }

    protected Stream<JsonElement> parse(InputStream in, boolean isProbe) {
        JsonElementSequenceIterator it = new JsonElementSequenceIterator(gson, gson.newJsonReader(new InputStreamReader(in)));
        return Streams.stream(it).onClose(it::close);
    }
}
