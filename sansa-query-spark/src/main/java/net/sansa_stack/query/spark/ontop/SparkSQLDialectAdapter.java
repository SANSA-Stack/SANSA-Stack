package net.sansa_stack.query.spark.ontop;

import it.unibz.inf.ontop.answering.reformulation.generation.dialect.impl.H2SQLDialectAdapter;
import it.unibz.inf.ontop.answering.reformulation.generation.dialect.impl.Mysql2SQLDialectAdapter;

/**
 * @author Lorenz Buehmann
 */
public class SparkSQLDialectAdapter extends Mysql2SQLDialectAdapter {

    @Override
    public String sqlSlice(long limit, long offset) {
        if(limit == 0){
            return "LIMIT 0";
        }

        if (limit < 0)  {
            if (offset <= 0 ) {
                return "";
            } else {
                return String.format("OFFSET %d ROWS", offset);
            }
        } else {
            if (offset < 0) {
                // If the offset is not specified
                return String.format("LIMIT %d", limit);
            } else {
                return String.format("OFFSET %d ROWS\nLIMIT %d", offset, limit);
            }
        }
    }

    public String SHA1(String str) {
        return String.format("SHA1(%s)", str);
    }

    public String SHA256(String str) {
        return String.format("SHA2(%s, 256)", str);
    }

    public String SHA512(String str) {
        return String.format("SHA2(%s, 512)", str);
    }

    public String MD5(String str) {
        return String.format("MD5(%s)", str);
    }

    public String strLength(String str) {
        return String.format("CHAR_LENGTH(%s)", str);
    }

    public String strUuid() {
        return "UUID()";
    }

    public String uuid() {
        return this.strConcat(new String[]{"'urn:uuid:'", "UUID()"});
    }

    @Override
    public String dateNow() {
        return super.dateNow();
    }
}
