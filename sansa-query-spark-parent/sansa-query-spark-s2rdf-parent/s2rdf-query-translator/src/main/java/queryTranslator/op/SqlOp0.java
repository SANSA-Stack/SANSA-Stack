package queryTranslator.op;

import java.util.ArrayList;
import java.util.HashMap;

import org.apache.jena.shared.PrefixMapping;

/**
 *
 * @author Antony Neu
 */
public abstract class SqlOp0 extends SqlBase {

    protected SqlOp0(PrefixMapping _prefixes) {
        prefixes = _prefixes;
        resultSchema = new HashMap<String,String[]>();
    }

}
