package queryTranslator.op;

import java.util.ArrayList;
import java.util.Map;

import queryTranslator.SqlOpVisitor;
import queryTranslator.Tags;


import com.hp.hpl.jena.shared.PrefixMapping;
import com.hp.hpl.jena.sparql.algebra.op.OpSequence;

/**
 *
 * @author Antony Neu
 */
public class SqlSequence extends SqlOpN {

    private final OpSequence opSequence;
    private ArrayList<String> intermediateSchema;


    public SqlSequence(OpSequence _opSequence, PrefixMapping _prefixes) {
        super(_prefixes);
        opSequence = _opSequence;
        resultName = Tags.SEQUENCE;
    }


    @Override
    public void visit(SqlOpVisitor sqlOpVisitor) {
    	sqlOpVisitor.visit(this);
    }

    public void setSchema(Map<String, String[]> schema){
    	this.resultSchema = schema;
 
    }
    
}
