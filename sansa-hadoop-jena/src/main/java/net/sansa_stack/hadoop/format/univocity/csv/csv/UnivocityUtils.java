package net.sansa_stack.hadoop.format.univocity.csv.csv;

import java.io.IOException;
import java.io.Reader;
import java.util.function.Supplier;

import com.univocity.parsers.common.CommonParserSettings;
import com.univocity.parsers.csv.CsvFormat;
import com.univocity.parsers.csv.CsvParser;
import com.univocity.parsers.csv.CsvParserSettings;

public class UnivocityUtils {
    
    public static boolean isDetectionNeeded(CsvParserSettings settings) {
    	boolean result = settings.isLineSeparatorDetectionEnabled()
    			|| settings.isDelimiterDetectionEnabled()
    			|| settings.isQuoteDetectionEnabled();

    	return result;
    }

    public static boolean isDetectionNeeded(CommonParserSettings<?> settings) {
    	boolean result = settings.isLineSeparatorDetectionEnabled();
    	return result;
    }

    public static CsvFormat detectFormat(CsvParser parser, Supplier<Reader> readerSupp) throws IOException {
    	CsvFormat result;
    	try (Reader reader = readerSupp.get()) {
	         parser.beginParsing(reader);
	         result = parser.getDetectedFormat();
        } finally {
	         parser.stopParsing();	    	
	    }
    	return result;
    }

}
