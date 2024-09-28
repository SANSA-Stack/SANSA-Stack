package net.sansa_stack.hadoop;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.junit.Test;

import java.io.IOException;
import java.io.StringReader;

/** These are not really tests but more experiments about how the parser behaves in edge cases */
public class CommonsCsvTests {
    // @Test
    public void testPercentEncoding() throws IOException {
        String line = "/sparql?query=ASK%0AWHERE%0A++%7B+%3Fs+%3Chttp%3A%2F%2Fnonsensical.com%2F1%2F1583472762380%3E+%3Chttp%3A%2F%2Fnonsensical.com%2F2%2F1583472762380%3E%7D%0A,pharmgkb.bio2 rdf.org,SPARQLES client using HTTPClient/4.2.3 (https://github.com/pyvandenbussche/sparqles),2020-03-06T05:32:47.473Z";
        CSVParser parser = CSVFormat.EXCEL.parse(new StringReader(line));
        parser.stream().flatMap(record -> record.stream()).forEach(System.out::println);
    }

    // @Test
    public void testLineFeedInUnquotedField() throws IOException {
        String line = "/sparql?query=ASK\rWHERE%0A++%7B+%3Fs+%3Chttp%3A%2F%2Fnonsensical.com%2F1%2F1583472762380%3E+%3Chttp%3A%2F%2Fnonsensical.com%2F2%2F1583472762380%3E%7D%0A,pharmgkb.bio2 rdf.org,SPARQLES client using HTTPClient/4.2.3 (https://github.com/pyvandenbussche/sparqles),2020-03-06T05:32:47.473Z";
        CSVParser parser = CSVFormat.Builder.create(CSVFormat.EXCEL).setRecordSeparator('\n').build()
                .parse(new StringReader(line));
        parser.stream().flatMap(record -> record.stream()).forEach(field -> System.out.println("Field: " + field));
    }
}
