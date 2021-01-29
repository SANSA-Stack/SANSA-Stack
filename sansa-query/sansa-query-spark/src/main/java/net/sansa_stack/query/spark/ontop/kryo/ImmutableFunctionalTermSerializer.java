package net.sansa_stack.query.spark.ontop.kryo;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.Serializer;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import it.unibz.inf.ontop.com.google.common.collect.*;
import it.unibz.inf.ontop.model.term.ImmutableFunctionalTerm;
import it.unibz.inf.ontop.model.term.TermFactory;
import it.unibz.inf.ontop.model.term.functionsymbol.FunctionSymbol;
import it.unibz.inf.ontop.model.term.impl.ImmutableFunctionalTermImpl;
import it.unibz.inf.ontop.model.term.impl.NonGroundFunctionalTermImpl;

import net.sansa_stack.query.spark.ontop.OntopConnection;
import net.sansa_stack.query.spark.ontop.OntopConnection$;

/**
 * A kryo {@link Serializer} for guava-libraries {@link ImmutableList}.
 */
public class ImmutableFunctionalTermSerializer extends Serializer<ImmutableFunctionalTerm> {

    private static final boolean DOES_NOT_ACCEPT_NULL = false;
    private static final boolean IMMUTABLE = true;
    private final String ontopSessionID;

    public ImmutableFunctionalTermSerializer(String ontopSessionID) {
        super(DOES_NOT_ACCEPT_NULL, IMMUTABLE);
        this.ontopSessionID = ontopSessionID;
    }

    @Override
    public void write(Kryo kryo, Output output, ImmutableFunctionalTerm object) {
        kryo.writeClassAndObject(output, object.getFunctionSymbol());
        kryo.writeClassAndObject(output, object.getTerms());
    }

    @Override
    public ImmutableFunctionalTerm read(Kryo kryo, Input input, Class<ImmutableFunctionalTerm> type) {
        final FunctionSymbol functionSymbol = (FunctionSymbol) kryo.readClassAndObject(input);
        final ImmutableList<? extends it.unibz.inf.ontop.model.term.ImmutableTerm> terms = (ImmutableList<? extends it.unibz.inf.ontop.model.term.ImmutableTerm>) kryo.readClassAndObject(input);

        OntopConnection$ conn = OntopConnection$.MODULE$;
        TermFactory termFactory = conn.configs().get(ontopSessionID).get().getTermFactory();
        ImmutableFunctionalTerm term = termFactory.getImmutableFunctionalTerm(functionSymbol, terms);

        return term;
    }

    /**
     * Creates a new {@link ImmutableFunctionalTermSerializer} and registers its serializer
     * for the several ImmutableFunctionalTerm related classes.
     *
     * @param kryo the {@link Kryo} instance to set the serializer on
     */
    public static void registerSerializers(final Kryo kryo, String ontopSessionID) {

        final ImmutableFunctionalTermSerializer serializer = new ImmutableFunctionalTermSerializer(ontopSessionID);

        kryo.register(ImmutableFunctionalTermImpl.class, serializer);
        kryo.register(NonGroundFunctionalTermImpl.class, serializer);
        kryo.register(ImmutableFunctionalTerm.class, serializer);

    }
}