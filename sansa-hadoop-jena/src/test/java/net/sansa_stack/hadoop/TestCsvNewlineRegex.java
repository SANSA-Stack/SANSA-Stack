package net.sansa_stack.hadoop;

import net.sansa_stack.hadoop.core.pattern.CustomMatcher;
import net.sansa_stack.hadoop.core.pattern.CustomPattern;
import net.sansa_stack.hadoop.core.pattern.CustomPatternCsv;
import net.sansa_stack.hadoop.core.pattern.CustomPatternJava;
import net.sansa_stack.hadoop.format.univocity.csv.csv.RecordReaderCsv;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class TestCsvNewlineRegex {

    public static CustomPattern createPattern() {
        CustomPattern result;
        // A csv record could look like
        // foo,"""foo->
        // bar"
        // baz,bay
        int cand = 3;
        switch (cand) {
            case 0:
                return CustomPatternJava.compile("(?<=\n(?!((?<![^\"]\"[^\"]).){0,50000}\"(\r?\n|,|$))).",
                        Pattern.DOTALL);

                // when going back from the quote char before the cell delimiters [,\n$]
                //
            case 1: {
                // Match an effective quote: A quote that is preceeded by an even number of quotes
                String eQuote = "[^\"](\"\"){0,10}\"";

                // Match up to n characters not preceeded by an effective quote
                String noLeadingEQuotedChar = "((?<!${eQuote}(?!\")).){0,50000}";

                // Match a character following a newline but only if the following there is
                // no effective quote that is NOT preceeded by an effective quote
                String matchCharAfterNewline = "(?<=\n(?!${noLeadingEQuotedChar}${eQuote}(\r?\n|,|$))).";
//              String matchCharAfterNewline = "(?<=\n(?!${noLeadingEQuotedChar}${eQuote}(\r?\n|,|$))).";

                String complete = matchCharAfterNewline
                        .replace("${noLeadingEQuotedChar}", noLeadingEQuotedChar)
                        .replace("${eQuote}", eQuote);

                // System.out.println("complete:" + complete);

                return CustomPatternJava.compile(
                        // There must not be an unescaped quote char before line limiter
                        // without a prior unescaped quote char
                        //"(?<=(\n|^)(?!((?<![^\"](\"\"){0,10}\"(?!\")).){0,50000}[^\"](\"\"){0,10}\"(\r?\n|,|$))).",
                        complete,
                        Pattern.DOTALL);
/*
                return Pattern.compile(
                        "(?<=\n(?!((?<!(?<![^\"](\"\"){0,10}\")).){0," + maxCharsPerColumn + "}\"(\r?\n|,|$))).",
                        Pattern.DOTALL);
*/
            }
            case 2: {
                // Match the first quote in a sequence of quotes:
                // A quote that is
                // - not preceded by a quote
                // - is followed by an even number of quotes followed by a non-quote char or end-of-line
                String equoteFirst = "((?<!\")\"(?=(\"\"){0,10}([^\"]|$)))";
                String equoteLast = "((?<=(^|[^\"])(\"\"){0,10})\"(?!\"))";

                // A character not preceded by an effective quote
                String unequotedChar = "((?<!${equoteLast}).)";

                // Match a character following a newline but only if the following there is
                // no effective quote that is NOT preceeded by an effective quote
                String matchCharAfterNewline = "(?<=\n(?!(?<!${equoteLast}).{0,50000}${equoteFirst}(\r?\n|,|$))).";
//              String matchCharAfterNewline = "(?<=\n(?!${noLeadingEQuotedChar}${eQuote}(\r?\n|,|$))).";

                String complete = matchCharAfterNewline
                        .replace("${unequotedChar}", unequotedChar)
                        .replace("${equoteFirst}", equoteFirst)
                        .replace("${equoteLast}", equoteLast)
                        ;

                // System.out.println("complete:" + complete);

                return CustomPatternJava.compile(
                        // There must not be an unescaped quote char before line limiter
                        // without a prior unescaped quote char
                        //"(?<=(\n|^)(?!((?<![^\"](\"\"){0,10}\"(?!\")).){0,50000}[^\"](\"\"){0,10}\"(\r?\n|,|$))).",
                        complete,
                        Pattern.DOTALL);
            }
            case 3: return new CustomPatternCsv();
            default:
                return null;
        }
    }

    @Test
    public void test1() {

        /*
        Pattern testPattern = Pattern.compile("\n");
        Matcher testMatcher = testPattern.matcher("abcd");
        System.out.println(testMatcher.group());
         */

        CustomPattern pattern = new CustomPatternCsv();

                //createPattern();

        // expected: 4, 8, 35
        String input0 = String.join("\n",
                "a,b",
                "x,y",
                "\"\"\"this\nis\nmultiline\"\"\", d",
                "e,f"
        );

        // expected 4, 8, 18
        String input1 = String.join("\n",
                "a,b",
                "x,y",
                "\"\"\"\"\"\", d",
                "e,f"
        );

        // 17
        String input2 = String.join("\n",
                "a,b",
                "x,y",
                "\"\"\"\"\", d",
                "e,f"
        );


        List<String> inputs = Arrays.asList(
                input0,
                input1,
                input2);
        int i = 0;
        for (String input : inputs) {
            System.out.println("input #" + i++);
            CustomMatcher m = pattern.matcher(input);
            while (m.find()) {
                System.out.println("newline at pos: " + m.start() + " --- group: " + m.group());
            }
        }
    }
}
