package net.sansa_stack.rdf.spark.riot.lang;

import org.apache.jena.datatypes.RDFDatatype;
import org.apache.jena.graph.Node;
import org.apache.jena.graph.Triple;
import org.apache.jena.iri.IRI;
import org.apache.jena.riot.system.ErrorHandler;
import org.apache.jena.riot.system.FactoryRDF;
import org.apache.jena.riot.system.IRIResolver;
import org.apache.jena.riot.system.ParserProfile;
import org.apache.jena.riot.system.PrefixMap;
import org.apache.jena.riot.tokens.Token;
import org.apache.jena.sparql.core.Quad;

class NoErrorProfile implements ParserProfile {
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

	@Override
	public FactoryRDF getFactorRDF() {
		return base.getFactorRDF();
	}
}
