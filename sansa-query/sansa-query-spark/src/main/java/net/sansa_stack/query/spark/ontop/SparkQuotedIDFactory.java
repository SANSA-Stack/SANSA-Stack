package net.sansa_stack.query.spark.ontop;

import it.unibz.inf.ontop.dbschema.QuotedID;
import it.unibz.inf.ontop.dbschema.impl.QuotedIDImpl;
import it.unibz.inf.ontop.dbschema.impl.SQLStandardQuotedIDFactory;

import javax.annotation.Nonnull;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;

public class SparkQuotedIDFactory extends SQLStandardQuotedIDFactory {
    Constructor<QuotedIDImpl> constructor;

    {
        try {
            constructor = QuotedIDImpl.class.getDeclaredConstructor(String.class, String.class, boolean.class);
            constructor.setAccessible(true);
        } catch (NoSuchMethodException e) {
            e.printStackTrace();
        }
    }

    private boolean caseSensitiveTableNames;
    private static final String SQL_QUOTATION_STRING = "`";

    private QuotedIDImpl newQuotedID(@Nonnull String id, String quoteString, boolean caseSensitive) {
        try {
            return constructor.newInstance(id, quoteString, caseSensitive);
        } catch (InstantiationException | IllegalAccessException | InvocationTargetException e) {
            e.printStackTrace();
        }
        return null;
    }

    @Override
    protected QuotedID createFromString(@Nonnull String s) {
        // Backticks are tolerated for SparkSQL schema and table names, but not necessary
        if (s.startsWith(SQL_QUOTATION_STRING) && s.endsWith(SQL_QUOTATION_STRING))
            return newQuotedID(s.substring(1, s.length() - 1), SQL_QUOTATION_STRING, caseSensitiveTableNames);

        return newQuotedID(s, SQL_QUOTATION_STRING, caseSensitiveTableNames);
    }

    @Override
    public String getIDQuotationString() {
        return SQL_QUOTATION_STRING;
    }
}
