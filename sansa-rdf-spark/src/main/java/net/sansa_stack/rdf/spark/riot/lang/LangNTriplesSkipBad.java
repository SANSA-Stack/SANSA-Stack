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
import org.apache.jena.datatypes.RDFDatatype;
import org.apache.jena.graph.Node;
import org.apache.jena.graph.Triple;
import org.apache.jena.iri.IRI;
import org.apache.jena.riot.Lang;
import org.apache.jena.riot.RDFLanguages;
import org.apache.jena.riot.RiotException;
import org.apache.jena.riot.RiotParseException;
import org.apache.jena.riot.lang.LangNTuple;
import org.apache.jena.riot.system.*;
import org.apache.jena.riot.tokens.Token;
import org.apache.jena.riot.tokens.TokenType;
import org.apache.jena.riot.tokens.Tokenizer;
import org.apache.jena.sparql.core.Quad;
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

	static class NoErrorProfile implements ParserProfile {
		private final ParserProfile base;

		NoErrorProfile(ParserProfile base) {
			this.base = base;
		}

		@Override
		public String resolveIRI(String uriStr, long line, long col) {
			return base.resolveIRI(uriStr, line, col);
		}

		@Override
		public IRI makeIRI(String uriStr, long line, long col) {
			return base.makeIRI(uriStr, line, col);
		}

		@Override
		public void setIRIResolver(IRIResolver resolver) {
			base.setIRIResolver(resolver);
		}

		@Override
		public Triple createTriple(Node subject, Node predicate, Node object, long line, long col) {
			return base.createTriple(subject, predicate, object, line, col);
		}

		@Override
		public Quad createQuad(Node graph, Node subject, Node predicate, Node object, long line, long col) {
			return base.createQuad(graph, subject, predicate, object, line, col);
		}

		@Override
		public Node createURI(String uriStr, long line, long col) {
			return base.createURI(uriStr, line, col);
		}

		@Override
		public Node createTypedLiteral(String lexical, RDFDatatype datatype, long line, long col) {
			return base.createTypedLiteral(lexical, datatype, line, col);
		}

		@Override
		public Node createLangLiteral(String lexical, String langTag, long line, long col) {
			return base.createLangLiteral(lexical, langTag, line, col);
		}

		@Override
		public Node createStringLiteral(String lexical, long line, long col) {
			return base.createStringLiteral(lexical, line, col);
		}

		@Override
		public Node createBlankNode(Node scope, String label, long line, long col) {
			return base.createBlankNode(scope, label, line, col);
		}

		@Override
		public Node createBlankNode(Node scope, long line, long col) {
			return base.createBlankNode(scope, line, col);
		}

		@Override
		public Node createNodeFromToken(Node scope, Token token, long line, long col) {
			return base.createNodeFromToken(scope, token, line, col);
		}

		@Override
		public Node create(Node currentGraph, Token token) {
			return base.create(currentGraph, token);
		}

		@Override
		public boolean isStrictMode() {
			return false;
		}

		@Override
		public PrefixMap getPrefixMap() {
			return base.getPrefixMap();
		}

		@Override
		public ErrorHandler getErrorHandler() {
			return null;
		}
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
		    int i = 0;
		    try {
			    Token sToken = nextToken();
			    if (sToken.isEOF())
				    exception(sToken, "Premature end of file: %s", sToken);
			    i = 3;
			    checkIRIOrBNode(sToken);
			    i = 0;

			    Token pToken = nextToken();
			    if (pToken.isEOF())
				    exception(pToken, "Premature end of file: %s", pToken);
			    i = 2;
			    checkIRI(pToken);
			    i = 0;

			    Token oToken = nextToken();
			    if (oToken.isEOF())
				    exception(oToken, "Premature end of file: %s", oToken);
			    i = 1;
			    checkRDFTerm(oToken);
			    i = 0;

			    // Check in createTriple - but this is cheap so do it anyway.
			    Token x = nextToken();

			    if (x.getType() != TokenType.DOT)
				    exception(x, "Triple not terminated by DOT: %s", x);

			    Node s = tokenAsNode(sToken);
			    Node p = tokenAsNode(pToken);
			    Node o = tokenAsNode(oToken);
			    triple = profile.createTriple(s, p, o, sToken.getLine(), sToken.getColumn());
		    } catch (RiotParseException e) {
			    if (i > 0) {
				    ((TokenizerTextForgiving)tokens).skipLine();
				    nextToken();
			    } else {
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
