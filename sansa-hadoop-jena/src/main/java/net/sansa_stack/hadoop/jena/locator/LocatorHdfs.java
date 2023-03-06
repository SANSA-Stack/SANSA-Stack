package net.sansa_stack.hadoop.jena.locator;

import java.io.InputStream;
import java.util.Arrays;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.jena.atlas.web.ContentType;
import org.apache.jena.atlas.web.TypedInputStream;
import org.apache.jena.riot.RDFLanguages;
import org.apache.jena.riot.system.stream.LocatorURL;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Support for resources using the "hdfs:" scheme. */
public class LocatorHdfs extends LocatorURL {
    private static Logger log         = LoggerFactory.getLogger(LocatorHdfs.class) ;
    public static final String[] SCHEME_NAMES = {"hdfs"} ;

    protected FileSystem fileSystem;

    // This attribute exists in the base class but is private (Jena 3.17.0)
    protected String[] xschemeNames;

    public LocatorHdfs(FileSystem fileSystem) {
        this(fileSystem, SCHEME_NAMES);
    }

    public LocatorHdfs(FileSystem fileSystem, String[] schemeNames) {
        super(schemeNames) ;
        this.fileSystem = fileSystem;
        this.xschemeNames = schemeNames;
    }

    @Override
    protected Logger log() { return log ; }

    @Override
    public TypedInputStream performOpen(String uri) {
        TypedInputStream result = null;
        if (Arrays.stream(xschemeNames).anyMatch(schemaName -> uri.startsWith(schemaName))) {
            ContentType contentType = RDFLanguages.guessContentType(uri);
            Path path = new Path(uri);
            InputStream in;
            try {
                in = fileSystem.open(path);
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
            result = new TypedInputStream(in, contentType);
        }
         //   return HttpOp.execHttpGet(uri, WebContent.defaultRDFAcceptHeader) ;
        return result;
    }

    @Override
    public String getName() {
        return "LocatorHdfs" ;
    }

    @Override
    public int hashCode() {
        return 83 ;
    }
}
