package net.sansa_stack.rdf.benchmark.io;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.lang.reflect.Constructor;
import java.net.URL;
import java.nio.file.Files;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;

import org.aksw.commons.util.compress.MetaBZip2CompressorInputStream;
import org.apache.commons.io.IOUtils;
import org.apache.jena.graph.Graph;
import org.apache.jena.graph.Triple;
import org.apache.jena.mem.GraphMem;
import org.apache.jena.rdf.model.Model;
import org.apache.jena.rdf.model.ModelFactory;
import org.apache.jena.rdf.model.RDFErrorHandler;
import org.apache.jena.rdf.model.impl.NTripleReader;
import org.apache.jena.riot.Lang;
import org.apache.jena.riot.RDFDataMgr;
import org.apache.jena.riot.lang.PipedRDFIterator;
import org.apache.jena.riot.lang.PipedRDFStream;
import org.apache.jena.riot.lang.PipedTriplesStream;
import org.apache.jena.shared.SyntaxError;

import com.google.common.base.Stopwatch;
import com.google.common.collect.Streams;


public class SansaBenchRdfIo {
	
	public static void main(String[] args) throws Exception {
		File tmpDir = new File("/tmp");
		File file = new File(tmpDir, "2015-11-02-Abutters.way.sorted.nt");
		
		if(!file.exists()) {
			File tmpFile = new File(file.getPath() + ".tmp");
		
			try(InputStream in = new MetaBZip2CompressorInputStream(new URL("http://downloads.linkedgeodata.org/releases/2015-11-02/2015-11-02-Abutters.way.sorted.nt.bz2").openStream());
					OutputStream out = new FileOutputStream(tmpFile)) {
				IOUtils.copy(in, out);
				out.flush();
			}
			
			tmpFile.renameTo(file);
		}
			
		//File file = new File("/home/raven/tmp/2015-11-02-Abutters.way.sorted.nt");

		int runs = 2;
		
		Map<String, Callable<Long>> map = new LinkedHashMap<>();
		map.put("parseWhole", () -> parseFile(file).count());
		map.put("parseLineMap", () -> parseLineMap(file).count());
		map.put("parseLineFlatMap", () -> parseLineFlatMap(file).count());
		map.put("parseReader", () -> parseReader(file).count());

		for(Entry<String, Callable<Long>> entry : map.entrySet()) {		
			System.out.println("Running " + entry.getKey());
			for(int i = 0; i < runs; ++i) {
				Stopwatch sw = Stopwatch.createStarted();
				long count = entry.getValue().call();
				
				System.out.println("Time taken [" + entry.getKey() + ", "+ count + "] " + sw.stop().elapsed(TimeUnit.MILLISECONDS));
			}
		}
	}
	
	public static <T> T forceNew(Class<T> clazz) {
		try {
			Constructor<T> ctor;
			try {
				ctor = clazz.getConstructor();
			} catch(NoSuchMethodException e) {
				ctor = clazz.getDeclaredConstructor();
				ctor.setAccessible(true);
			}
			
			T result = ctor.newInstance();
			return result;
		} catch(Exception e) {
			throw new RuntimeException(e);
		}
	}

	
	
	public static Stream<Triple> parseReader(File file) throws FileNotFoundException, InterruptedException, ExecutionException {
        PipedRDFIterator<Triple> pipedRdfIterator = new PipedRDFIterator<>();

        PipedRDFStream<Triple> pipedRdfStream = new PipedTriplesStream(pipedRdfIterator);

        Graph g = new GraphMem() {
			public void add(Triple t) {
				//System.out.println("" + t);
				pipedRdfStream.triple(t);
			}
        };
        
		Model m = ModelFactory.createModelForGraph(g);
		NTripleReader reader = forceNew(NTripleReader.class);

		RDFErrorHandler handler = new RDFErrorHandler() {

			@Override
			public void warning(Exception e) {
				System.err.println("warn: " + e);
			}

			@Override
			public void error(Exception e) {
				System.err.println("error: " + e);
			}

			@Override
			public void fatalError(Exception e) {
				System.err.println("fatal: " + e);
			}
		};

		reader.setErrorHandler(handler);
		
        ExecutorService executor = Executors.newSingleThreadExecutor();

        InputStream in = new FileInputStream(file);

        Future<?> foo = executor.submit(() -> {
    		pipedRdfStream.start();
    		try {
    			reader.read(m, in, "http://example.org");
    		} catch (SyntaxError e) {
    			// Silently ignore syntax errors, as they were reported to the error handler anyway
    		}
    		pipedRdfStream.finish();        	
        });
        
        Stream<Triple> result = Streams.stream(pipedRdfIterator)
        		.onClose(() -> {
        			foo.cancel(true);
					try {
						foo.get();
					} catch (Exception e) {
						e.printStackTrace();
					}
        		});

        return result;
	}
	
	public static Stream<Triple> parseFile(File file) throws FileNotFoundException {		
		Stream<Triple> result = Streams.stream(RDFDataMgr
				.createIteratorTriples(
						new FileInputStream(file), Lang.NTRIPLES, "http://example.org"));
		return result;
	}
	
	public static Stream<Triple> parseLineMap(File file) throws IOException {
		return Files.lines(file.toPath())
				.map(line -> RDFDataMgr
						.createIteratorTriples(new ByteArrayInputStream(
								line.getBytes()), Lang.NTRIPLES, "http://example.org").next())
		;
	}

	public static Stream<Triple> parseLineFlatMap(File file) throws IOException {
		return Files.lines(file.toPath())
				.flatMap(line -> Streams
						.stream(RDFDataMgr
								.createIteratorTriples(new ByteArrayInputStream(
										line.getBytes()), Lang.NTRIPLES, "http://example.org")))
		;
	}
}
