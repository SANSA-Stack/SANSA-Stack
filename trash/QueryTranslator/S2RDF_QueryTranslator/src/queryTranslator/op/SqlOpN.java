package queryTranslator.op;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;

import queryTranslator.sql.SqlStatement;


import com.hp.hpl.jena.shared.PrefixMapping;

/**
 *
 * @author Antony Neu
 */
public abstract class SqlOpN extends SqlBase {

    protected List<SqlOp> elements = new ArrayList<SqlOp>();


    protected SqlOpN(PrefixMapping _prefixes) {
        prefixes = _prefixes;
        resultSchema = new HashMap<String, String[]>();
    }

    public SqlOp get(int index) {
        return elements.get(index);
    }

    public List<SqlOp> getElements() {
        return elements;
    }

    public void add(SqlOp op) {
        elements.add(op);
    }

    public void add(int index, SqlOp op) {
        elements.add(index, op);
    }

    public Iterator<SqlOp> iterator() {
        return elements.iterator();
    }

    public int size() {
        return elements.size();
    }

 
    
}
