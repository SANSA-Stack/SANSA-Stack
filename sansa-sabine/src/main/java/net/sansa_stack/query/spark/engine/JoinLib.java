package net.sansa_stack.query.spark.engine;

import org.apache.jena.graph.Node;
import org.apache.jena.sparql.core.Var;
import org.apache.jena.sparql.engine.binding.Binding;

/** Copy from Jena because this class is not public; also the hashing here operates on Iterable<Var> rather than JoinKey */

/** Internal operations in support of join algorithms. */
public class JoinLib {

    /** Control stats output / development use */
    static final boolean JOIN_EXPLAIN = false;

    // No hash key marker.
    public static final Long noKeyHash = null; //new Object() ;
    public static final long nullHashCode = 5 ;

    public static long hash(Var v, Node x) {
        long h = 17;
        if ( v != null )
            h = h ^ v.hashCode();
        if ( x != null )
            h = h ^ x.hashCode();
        return h;
    }

    public static Long hash(Iterable<Var> joinKey, Binding row) {
        long x = 31 ;
        boolean seenJoinKeyVar = false ;
        // Neutral to order in the set.
        for ( Var v : joinKey ) {
            Node value = row.get(v) ;
            long h = nullHashCode ;
            if ( value != null ) {
                seenJoinKeyVar = true ;
                h = hash(v, value) ;
            } else {
                // In join key, not in row.
            }

            x = x ^ h ;
        }
        if ( ! seenJoinKeyVar )
            return noKeyHash ;
        return x ;
    }
}
