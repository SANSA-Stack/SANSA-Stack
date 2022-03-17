package net.sansa_stack.hadoop;

import net.sansa_stack.hadoop.core.pattern.CustomMatcher;
import net.sansa_stack.hadoop.core.pattern.CustomPattern;
import net.sansa_stack.hadoop.format.univocity.csv.csv.RecordReaderCsv;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;

public class TestCsvNewlineRegex {

    @Test
    public void test1() {
        CustomPattern pattern = RecordReaderCsv.createStartOfCsvRecordPattern(1000);

        String input1 = String.join("\n",
                "a,b",
                "x,y",
                "\"\"\"this\nis\nmultiline\"\"\", d",
                "e,f"
        );

        String input2 = String.join("\n",
                "a,b",
                "x,y",
                "\"\"\"\"\"\", d",
                "e,f"
        );


        List<String> inputs = Arrays.asList(input1, input2);
        int i = 0;
        for (String input : inputs) {
            System.out.println("input #" + i++);
            CustomMatcher m = pattern.matcher(input);
            while (m.find()) {
                System.out.println(m.start() + ": " + m.group());
            }
        }
    }
}
