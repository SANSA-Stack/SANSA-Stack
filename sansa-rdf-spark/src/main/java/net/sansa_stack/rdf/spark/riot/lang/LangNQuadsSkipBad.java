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

import net.sansa_stack.rdf.spark.riot.tokens.TokenizerTextForgiving;
import org.apache.jena.atlas.iterator.PeekIterator;
import org.apache.jena.graph.Node;
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
import org.apache.jena.sparql.core.Quad;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Iterator;
import java.util.NoSuchElementException;

/**
 * N-Quads.
 *
 * @see <a href="http://www.w3.org/TR/n-quads/">http://www.w3.org/TR/n-quads/</a>
 */
public final class LangNQuadsSkipBad implements Iterator<Quad>
{
    private static Logger messageLog = LoggerFactory.getLogger(LangNQuadsSkipBad.class) ;
	private RiotException bad = null;

	private PeekIterator<Quad> base = null;

	public LangNQuadsSkipBad(TokenizerTextForgiving tokens, ParserProfile profile, StreamRDF dest) {
		try { base = new PeekIterator<>(new Wrapper(tokens, profile, dest)); }
		catch (RiotException e) {
			bad = e;
			profile.getErrorHandler().warning(e.getMessage(), -1, -1);
		}
	}

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
	public Quad next() {
		if (bad != null) { throw bad; }
		while ( base.peek() == null ) {  base.next(); }
		return base.next();
	}

	@Override
	public void remove() {
		base.remove();
	}


	static class Wrapper extends LangNTuple<Quad> {

		// Null for no graph.
		private Node currentGraph = null ;

		/** shadow parent errorHandler */
		private ErrorHandler errorHandler;
    	Wrapper(Tokenizer tokens, ParserProfile profile, StreamRDF dest) {
		    super(tokens, new NoErrorProfile(profile), dest) ;
		    this.errorHandler = profile.getErrorHandler();
	    }

	    /** Method to parse the whole stream of triples, sending each to the sink */
		@Override
		protected final void runParser()
		{
			while(hasNext())
			{
				Quad x = parseOne() ;
				if ( x != null )
					dest.quad(x) ;
			}
		}

		@Override
		protected final Quad parseOne()
		{
//			System.err.println("\nparseone");
			boolean needSkip = false;
			try {
				Token sToken = nextToken() ;
//				System.err.println("stoken="+sToken);
				if ( sToken.getType() == TokenType.EOF )
					exception(sToken, "Premature end of file: %s", sToken) ;
				needSkip = true;
				checkIRIOrBNode(sToken) ;
				needSkip = false;

				Token pToken = nextToken() ;
//				System.err.println("ptoken="+pToken);
				if ( pToken.getType() == TokenType.EOF )
					exception(pToken, "Premature end of file: %s", pToken) ;
				needSkip = true;
				checkIRI(pToken) ;
				needSkip = false;

				Token oToken = nextToken() ;
//				System.err.println("otoken="+oToken);
				if ( oToken.getType() == TokenType.EOF )
					exception(oToken, "Premature end of file: %s", oToken) ;
				needSkip = true;
				checkRDFTerm(oToken) ;
				needSkip = false;

				Token xToken = nextToken() ;    // Maybe DOT
//				System.err.println("xtoken="+xToken);
				if ( xToken.getType() == TokenType.EOF )
					exception(xToken, "Premature end of file: Quad not terminated by DOT: %s", xToken) ;

				// Process graph node first, before S,P,O
				// to set bnode label scope (if not global)
				Node c = null ;
				if ( xToken.getType() != TokenType.DOT )
				{
					// Allow bNodes for graph names.
					needSkip = true;
					checkIRIOrBNode(xToken) ;
					needSkip = false;
					// Allow only IRIs
					//checkIRI(xToken) ;
					c = tokenAsNode(xToken) ;
					xToken = nextToken() ;
//					System.err.println("ztoken="+xToken);
					currentGraph = c ;
				}
				else
				{
					c = Quad.defaultGraphNodeGenerated ;
					currentGraph = null ;
				}

				// createQuad may also check but these checks are cheap and do form syntax errors.
				// xToken already checked.

				Node s = tokenAsNode(sToken) ;
				Node p = tokenAsNode(pToken) ;
				Node o = tokenAsNode(oToken) ;

				// Check end of tuple.

				needSkip = true;
				if ( xToken.getType() != TokenType.DOT )
					exception(xToken, "Quad not terminated by DOT: %s", xToken) ;

				return profile.createQuad(c, s, p, o, sToken.getLine(), sToken.getColumn()) ;
			} catch (RiotParseException e) {
				errorHandler.warning("skipping line " + e.getOriginalMessage(), e.getLine(), e.getCol());
				if(needSkip) {
					((TokenizerTextForgiving)tokens).skipLine();
					nextToken();
				}
			} catch (NullPointerException e2) {
				errorHandler.warning(e2.getMessage(), currLine, currCol);
			}
			return null;
		}

//	    @Override
//	    protected final Triple parseOne() {
//		    Triple triple = null;
//		    int i = 0;
//		    try {
//			    Token sToken = nextToken();
//			    if (sToken.isEOF())
//				    exception(sToken, "Premature end of file: %s", sToken);
//			    i = 3;
//			    checkIRIOrBNode(sToken);
//			    i = 0;
//
//			    Token pToken = nextToken();
//			    if (pToken.isEOF())
//				    exception(pToken, "Premature end of file: %s", pToken);
//			    i = 2;
//			    checkIRI(pToken);
//			    i = 0;
//
//			    Token oToken = nextToken();
//			    if (oToken.isEOF())
//				    exception(oToken, "Premature end of file: %s", oToken);
//			    i = 1;
//			    checkRDFTerm(oToken);
//			    i = 0;
//
//			    // Check in createTriple - but this is cheap so do it anyway.
//			    Token x = nextToken();
//
//			    if (x.getType() != TokenType.DOT)
//				    exception(x, "Triple not terminated by DOT: %s", x);
//
//			    Node s = tokenAsNode(sToken);
//			    Node p = tokenAsNode(pToken);
//			    Node o = tokenAsNode(oToken);
//			    triple = profile.createTriple(s, p, o, sToken.getLine(), sToken.getColumn());
//		    } catch (RiotParseException e) {
//			    if (i > 0) {
//				    ((TokenizerTextForgiving)tokens).skipLine();
//				    nextToken();
//			    } else {
//				    /** this is handled by {@link TokenizerTextForgiving} */
//			    }
//		    } catch (NullPointerException e2) {
//			    errorHandler.warning(e2.getMessage(), currLine, currCol);
//		    }
//		    return triple;
//	    }

	    @Override
	    protected final Node tokenAsNode(Token token) {
		    return profile.create(null, token) ;
	    }

	    @Override
	    public Lang getLang()   { return RDFLanguages.NQUADS ; }

    }

}
