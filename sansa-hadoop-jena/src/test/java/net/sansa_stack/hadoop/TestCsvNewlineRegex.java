package net.sansa_stack.hadoop;

import net.sansa_stack.hadoop.core.pattern.CustomMatcher;
import net.sansa_stack.hadoop.core.pattern.CustomPattern;
import net.sansa_stack.hadoop.core.pattern.CustomPatternCsv;
import net.sansa_stack.hadoop.core.pattern.CustomPatternJava;
import net.sansa_stack.hadoop.format.univocity.csv.csv.RecordReaderCsv;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;
import java.util.regex.Pattern;

public class TestCsvNewlineRegex {

    public static CustomPattern createPattern() {
        CustomPattern result;
        // A csv record could look like
        // foo,"""foo->
        // bar"
        // baz,bay
        int cand = 0;
        switch (cand) {
            case 0:
                return CustomPatternJava.compile("(?<=\n(?!((?<![^\"]\"[^\"]).){0,50000}\"(\r?\n|,|$))).",
                        Pattern.DOTALL);

                // when going back from the quote char before the cell delimiters [,\n$]
                //
            case 1:
                return CustomPatternJava.compile(
                        //"(?<=\n(?!((?<![^\"]\"(\"\"){0,10}[^\"]).){0," + maxCharsPerColumn + "}(\"\"){0,10}\"(\r?\n|,|$))).",
                        // "(?<=(\n|^)(?!((?<![^\"](\"\"){0,10}\"[^\"]).){0," + maxCharsPerColumn + "}(\"\"){0,10}\"(\r?\n|,|$))).",

                        // There must not be an unescaped quote char sequence followed by a quote char
                        "(?<=(\n|^)(?!((?<![^\"](\"\"){0,10}\"(?!\")).){0,50000}(\"\"){0,10}\"(\r?\n|,|$))).",

                        Pattern.DOTALL);
/*
                return Pattern.compile(
                        "(?<=\n(?!((?<!(?<![^\"](\"\"){0,10}\")).){0," + maxCharsPerColumn + "}\"(\r?\n|,|$))).",
                        Pattern.DOTALL);
*/

            default:
                return null;
        }
    }

    @Test
    public void test1() {
        CustomPattern pattern = createPattern();

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
