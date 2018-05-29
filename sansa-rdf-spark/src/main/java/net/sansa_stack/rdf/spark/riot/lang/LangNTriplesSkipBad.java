/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package net.sansa_stack.rdf.spark.riot.lang;

import java.util.Iterator;
import java.util.NoSuchElementException;

import org.apache.jena.atlas.iterator.PeekIterator;
import org.apache.jena.graph.Node;
import org.apache.jena.graph.Triple;
import org.apache.jena.riot.Lang;
import org.apache.jena.riot.RDFLanguages;
import org.apache.jena.riot.RiotException;
import org.apache.jena.riot.RiotParseException;
import org.apache.jena.riot.lang.LangNTuple;
import org.apache.jena.riot.system.ErrorHandler;
import org.apache.jena.riot.system.ParserProfile;
import org.apache.jena.riot.system.StreamRDF;
import org.apache.jena.riot.tokens.Token;
import org.apache.jena.riot.tokens.TokenType;
import org.apache.jena.riot.tokens.Tokenizer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import net.sansa_stack.rdf.spark.riot.tokens.TokenizerTextForgiving;

/**
 * N-Triples.
 * 
 * @see <a href="http://www.w3.org/TR/n-triples/">http://www.w3.org/TR/n-triples/</a>
 */
public final class LangNTriplesSkipBad implements Iterator<Triple>
{
    private static Logger messageLog = LoggerFactory.getLogger(LangNTriplesSkipBad.class) ;
	private RiotException bad = null;

	@Override
	public boolean hasNext() {
		if (base == null) { return false; }
		try {
			while (base.peek() == null) {
				base.next();
			}
		} catch (NoSuchElementException e) {
			return false;
		}
		return base.hasNext();
	}

	@Override
	public Triple next() {
		if (bad != null) { throw bad; }
		while ( base.peek() == null ) {  base.next(); }
		return base.next();
	}

	@Override
	public void remove() {
		base.remove();
	}

	static class Wrapper extends LangNTuple<Triple> {
		/** shadow parent errorHandler */
		private ErrorHandler errorHandler;
    	Wrapper(Tokenizer tokens, ParserProfile profile, StreamRDF dest) {
		    super(tokens, new NoErrorProfile(profile), dest) ;
		    this.errorHandler = profile.getErrorHandler();
	    }

	    /** Method to parse the whole stream of triples, sending each to the sink */
	    @Override
	    protected final void runParser() {
		    while (hasNext()) {
			    Triple x = parseOne();
			    if ( x != null )
				    dest.triple(x);
		    }
	    }

	    @Override
	    protected final Triple parseOne() {
		    Triple triple = null;
		    boolean needSkip = false;
//			System.err.println("process1");
		    try {
			    Token sToken = nextToken();
//				System.err.println("stoken="+sToken);
			    if (sToken.isEOF())
				    exception(sToken, "Premature end of file: %s", sToken);
			    needSkip = true;
			    checkIRIOrBNode(sToken);
			    needSkip = false;

			    Token pToken = nextToken();
//				System.err.println("ptoken="+pToken);
			    if (pToken.isEOF())
				    exception(pToken, "Premature end of file: %s", pToken);
				needSkip = true;
			    checkIRI(pToken);
				needSkip = false;

			    Token oToken = nextToken();
//				System.err.println("otoken="+oToken);
			    if (oToken.isEOF())
				    exception(oToken, "Premature end of file: %s", oToken);
				needSkip = true;
			    checkRDFTerm(oToken);
				needSkip = false;

			    // Check in createTriple - but this is cheap so do it anyway.
			    Token x = nextToken();
//				System.err.println("ztoken="+x);

			    if (x.getType() != TokenType.DOT)
				    exception(x, "Triple not terminated by DOT: %s", x);

			    Node s = tokenAsNode(sToken);
			    Node p = tokenAsNode(pToken);
			    Node o = tokenAsNode(oToken);
			    triple = profile.createTriple(s, p, o, sToken.getLine(), sToken.getColumn());
		    } catch (RiotParseException e) {
			    if (needSkip) {
//					System.err.println("skipping..."+e.getMessage());
				    ((TokenizerTextForgiving)tokens).skipLine();
				    nextToken();
			    } else {
//					System.err.println("bad:"+e.getMessage());
				    /** this is handled by {@link TokenizerTextForgiving} */
			    }
		    } catch (NullPointerException e2) {
			    errorHandler.warning(e2.getMessage(), currLine, currCol);
		    }
		    return triple;
	    }

	    @Override
	    protected final Node tokenAsNode(Token token) {
		    return profile.create(null, token) ;
	    }

	    @Override
	    public Lang getLang()   { return RDFLanguages.NTRIPLES ; }

    }

    private PeekIterator<Triple> base = null;

    public LangNTriplesSkipBad(TokenizerTextForgiving tokens, ParserProfile profile, StreamRDF dest) {
    	try { base = new PeekIterator<>(new Wrapper(tokens, profile, dest)); }
    	catch (RiotException e) {
    		bad = e;
    		profile.getErrorHandler().warning(e.getMessage(), -1, -1);
    	}
    }


}
