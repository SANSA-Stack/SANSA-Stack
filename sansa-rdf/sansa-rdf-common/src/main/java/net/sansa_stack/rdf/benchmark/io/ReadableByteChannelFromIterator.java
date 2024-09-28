package net.sansa_stack.rdf.benchmark.io;

import java.io.IOException;
import java.io.InputStream;
import java.nio.Buffer;
import java.nio.ByteBuffer;
import java.nio.channels.Channels;
import java.nio.channels.ReadableByteChannel;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.stream.Stream;

//import com.google.common.collect.Streams;
import com.google.common.collect.Streams;
import org.apache.jena.rdf.model.Model;
import org.apache.jena.rdf.model.ModelFactory;
import org.apache.jena.riot.Lang;
import org.apache.jena.riot.RDFDataMgr;
import org.apache.jena.riot.RDFFormat;

public class ReadableByteChannelFromIterator
        implements ReadableByteChannel
{
    public static void main(String[] args) {
        List<String> strs = Arrays.asList(
                "@prefix eg: <http://example.org/> .\n",
                "eg:a eg:b eg:c .\n",
                "eg:d eg:e eg:f .\n",
                "eg:g eg:h eg:i .\n");

        InputStream in = toInputStream(strs.stream());
        Model m = ModelFactory.createDefaultModel();

        //m.setNsPrefixes(PrefixMapping.Extended);
        //m.read(in, "http://example.org/", "turtle");
        RDFDataMgr.read(m, in, Lang.TURTLE);

        RDFDataMgr.write(System.err, m, RDFFormat.TURTLE_PRETTY);
    }

    public static InputStream toInputStream(Iterator<String> it) {
        return toInputStream(Streams.stream(it));
    }

    public static InputStream toInputStream(Stream<String> stream) {
        return Channels.newInputStream(new ReadableByteChannelFromIterator(
                stream
                        .map(s -> (s + '\n').getBytes())
//                        .map(String::getBytes)
                        .map(ByteBuffer::wrap)
                        .iterator()));
    }


    protected Iterator<ByteBuffer> it;
    protected boolean isOpen;
    protected ByteBuffer currentBuffer;

    private static char NL = '\n';

    public ReadableByteChannelFromIterator(Iterator<ByteBuffer> it) {
        super();
        this.it = it;
        this.isOpen = true;
    }

    @Override
    public boolean isOpen() {
        return isOpen;
    }

    @Override
    public void close() throws IOException {
        isOpen = false;
    }

    @Override
    public  int read(ByteBuffer dst) {
        int result = 0;

        int remaining = dst.remaining();
        //int readBytes = 0;

        while(remaining > 0) {
            int available = currentBuffer == null ? 0 : currentBuffer.remaining();

            if(available == 0) {
                // If we are at the last batch and have not read anything yet, we have reached the end
                if(!it.hasNext()) {
                    if(result == 0) {
                        result = -1;
                    }
                    break;
                } else {
                    currentBuffer = it.next();
                    continue;
                }
            }

            int toRead = Math.min(remaining, available);

            int off = currentBuffer.position();
            int newOff = off + toRead;

            ByteBuffer tmp = currentBuffer.duplicate();

            // Explicit casts from ByteBuffer to Buffer
            // due to Java 9+ compatibility issue; see https://github.com/eclipse/jetty.project/issues/3244
            ((Buffer)tmp).limit(newOff);
            dst.put(tmp);

            ((Buffer)currentBuffer).position(newOff);

            result += toRead;
            remaining -= toRead;
        }

        return result;
    }

}
