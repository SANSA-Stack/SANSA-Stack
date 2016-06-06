package queryTranslator.op;

import java.util.HashMap;

import queryTranslator.sql.SqlStatement;
import queryTranslator.sql.Select;


import com.hp.hpl.jena.shared.PrefixMapping;

/**
 *
 * @author Antony Neu
 */
public abstract class SqlOp1 extends SqlBase {

    protected SqlOp subOp;


    protected SqlOp1(SqlOp _subOp, PrefixMapping _prefixes) {
        subOp = _subOp;
        prefixes = _prefixes;
        resultSchema = new HashMap<String, String[]>();
    }

    public SqlOp getSubOp() {
        return subOp;
    }

    public abstract SqlStatement translate(String name, SqlStatement child);
    
}
