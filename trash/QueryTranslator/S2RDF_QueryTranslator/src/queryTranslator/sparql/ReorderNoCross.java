package queryTranslator.sparql;

import com.hp.hpl.jena.graph.Node;
import com.hp.hpl.jena.graph.Triple;
import com.hp.hpl.jena.sparql.core.BasicPattern;
import java.util.ArrayList;
import java.util.List;

/**
 *
 * @author Alexander Schaetzle
 */
public class ReorderNoCross {
    
    private final ArrayList<String> joinSchema;
    private final ArrayList<String> lastJoinVars;
    private BasicPattern inputPattern;
    private final BasicPattern outputPattern;

    public ReorderNoCross() {
        joinSchema = new ArrayList<String>();
        lastJoinVars = new ArrayList<String>();
        outputPattern = new BasicPattern();
    }


    public BasicPattern reorder(BasicPattern pattern) {
        inputPattern = pattern;
        List<Triple> triples = inputPattern.getList();

        int idx = chooseFirst();
        Triple triple = triples.get(idx);
        outputPattern.add(triple);
        joinSchema.addAll(getVarsOfTriple(triple));
        triples.remove(idx);

        while (!triples.isEmpty()) {
            idx = chooseNext();
            triple = triples.get(idx);
            outputPattern.add(triple);
            joinSchema.addAll(getVarsOfTriple(triple));
            triples.remove(idx);
        }

        return outputPattern;
    }

    private int chooseNext() {
        ArrayList<String> tripleVars;
        ArrayList<String> sharedVars;
        for (int i=0; i<inputPattern.size(); i++) {
            tripleVars = getVarsOfTriple(inputPattern.get(i));
            sharedVars = getSharedVars(joinSchema, tripleVars);
            if (lastJoinVars.size() > 0 && lastJoinVars.size() == sharedVars.size() && lastJoinVars.containsAll(sharedVars)) {
                // lastJoinVars remain unchanged
                return i;
            }
        }
        for (int i=0; i<inputPattern.size(); i++) {
            tripleVars = getVarsOfTriple(inputPattern.get(i));
            sharedVars = getSharedVars(joinSchema, tripleVars);
            if (sharedVars.size() > 0) {
                lastJoinVars.clear();
                lastJoinVars.addAll(sharedVars);
                return i;
            }
        }
        lastJoinVars.clear();
        return 0;
    }

    private int chooseFirst() {
        for (int i=0; i<inputPattern.size(); i++) {
            if (hasSharedVars(i)) {
                return i;
            }
        }
        return 0;
    }

    private boolean hasSharedVars(int triplePos) {
        Triple triple = inputPattern.get(triplePos);
        ArrayList<String> tripleVars = getVarsOfTriple(triple);
        for (int i=0; i<inputPattern.size(); i++) {
            if (i != triplePos && getSharedVars(getVarsOfTriple(inputPattern.get(i)), tripleVars).size() > 0) {
                return true;
            }
        }
        return false;
    }

    private ArrayList<String> getVarsOfTriple(Triple t) {
        ArrayList<String> vars = new ArrayList<String>();
        Node subject = t.getSubject();
        Node predicate = t.getPredicate();
        Node object = t.getObject();
        if(subject.isVariable())
            vars.add(subject.getName());
        if(predicate.isVariable())
            vars.add(predicate.getName());
        if(object.isVariable())
            vars.add(object.getName());
        return vars;
    }

    private ArrayList<String> getSharedVars(ArrayList<String> leftSchema, ArrayList<String> rightSchema) {
        ArrayList<String> sharedVars = new ArrayList<String>();
        for(int i=0; i<rightSchema.size(); i++) {
            if(leftSchema.contains(rightSchema.get(i)))
                sharedVars.add(rightSchema.get(i));
        }
        return sharedVars;
    }

}
