package net.sansa_stack.spark.io.rdf.output;

import org.aksw.jenax.arq.util.lang.RDFLanguagesEx;
import org.apache.jena.riot.Lang;

import java.util.Optional;

public class RddRowSetWriterSettings<SELF extends RddRowSetWriterSettings>
    extends RddWriterSettings<SELF>
{
    protected Lang outputLang;

    public SELF configureFrom(RddRowSetWriterSettings<?> other) {
        super.configureFrom(other);
        this.outputLang = outputLang;
        return self();
    }

    public SELF setOutputLang(Lang lang) {
        this.outputLang = lang;
        return self();
    }

    public SELF setOutputLang(String formatName) {
        Lang lang = formatName == null
                ? null
                : Optional.ofNullable(RDFLanguagesEx.findLang(formatName))
                .orElseThrow(() -> new IllegalArgumentException("Unknown format: " + formatName));
        return setOutputLang(lang);
    }


    public Lang getOutputLang() {
        return outputLang;
    }
}
