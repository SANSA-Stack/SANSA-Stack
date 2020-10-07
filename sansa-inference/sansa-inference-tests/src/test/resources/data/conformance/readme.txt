This is a test collection for the OWL 2 RDF-Based Semantics
and all its standard sub-semantics, including the 
OWL 2 RL/RDF rules language. 

Users interested only in applying the test cases will 
find different sub suites in the folder "subsuites/".

The folder "sources/" contains the development files
for all the testcases. The content of this folder will 
in general not be interesting for "normal" users.


== Folder "./" ==

There are currently three sub suites:

* owl2full: The whole test suite consisting of >700 test cases
  for the OWL 2 RDF-based semantics.
* owl2rl: The sub suite consisting of ~300 test cases
  for the OWL 2 RL/RDF rules, including the 87 test cases
  that have been approved by the W3C Working Group.
* rdfs: The sub suite consisting of test cases for RDFS.

Files: 
For each test case within one of the sub suite folders, 
there is a folder, containing the following files:

FILE: <TESTCASENAME>.metadata.properties 
Contains meta information about this testcase (Java XML-properties file)
* testcase.id: Name of the testcase
* testcase.description: A short description of the testcase. (purely informative)
* testcase.type: The testtype, one of "POSITIVE_ENTAILMENT", "INCONSISTENCY"
* testcase.entailmentregimeset: Always contains the string "OWL2RL", and always another minimal entailment regime to which the testcase applies
* testcase.authorlist: Michael and Kai :)
The most relevant information here is testcase.type, 
since this determines how to apply this testcase.

FILES <TESTCASENAME>.(premise|conclusion)?graph.(xml|ttl):
Depending on the testcase type, this is either 
the single input graph for (in)consistency checks,
or the premise and conclusion graph for (positive) entailment checks.
The graphs are available in RDF/XML ("xml") and Turtle ("ttl").

Usage: 
Should be pretty clear:
 
* For each testcase folder
** First read the metadata file and get the testcase type
** If testcase is an inconsistency check:
*** Read the single input graph, either in RDF/XML or Turtle
*** Apply a consistency check
** If testcase is a positive entailment check:
*** Read the premise graph and the conclusion graph
*** Apply an entailment check to the two graphs

If your reasoner works in forward-chaining mode, 
then there will be some resulting inference graph,
and you have to check the following way:

* for consistency tests: There should be some "error message" in the inference graph. Look it up.
* for entailment checks: check whether the conclusion graph is contained in the inference graph.

Caveats: 
There are some known issues:

* There are five tests that are dependent on bNodes, 
  which might not work as expected 
  (We know the problem, and will have to further check 
   what can be best done).
** rdfbased-ont-graph-form
** rdfbased-xtr-constraint-anonind-axioms
** rdfbased-xtr-constraint-anonind-forestlike
** rdfbased-xtr-subgraph-incomplete
** rdfbased-xtr-subgraph-monotonic

* In the Turtle encodings of the graphs,
  if there is a literal of one of the datatypes
  xsd:integer, xsd:decimal and xsd:double,
  these are not printed in the style
    "lexicalform"^^datatypeURI
  but only the lexical form is printed.
  This is allowed by the Turtle definition,
  but not every RDF parser is able to cope 
  with this. We will later see how to change
  this, but for now it's there.


== Folder "sources/" ==

The test suite is organized in several "test sections", 
each covering one general aspect of the RDF-Based Semantics, 
such as the semantic conditions. Each test section 
consists of several "test suite files", for example one suite 
per semantic condition table. The test suites themselves 
consist of several test cases, for example for the 
different entries in a semantic condition table. See the 
file "description-rdfbased.txt" for further information.

The following files and folders exist within folder "sources/":

status.txt						current state of development of the test collection
example-testsuite.txt 			example testsuite file, contains example testcases
testcollection-rdfbased/		toplevel folder of the whole test collection
	metadata.txt				some metadata to be incorporated in all generated tests
	description-rdfbased.txt	description for the whole test collection
	testsection-*/				folder of one test section, contains test suites
		description-*.txt		description of the test section
		testsuite-*.txt			a single test suite, contains test cases

== A list of Contributors ==

Different people have contributed in different ways, 
for example by reporting bugs. Here is a list of some,
ordered by family name:

Chris Dollin
Daniel Elenius
Ivan Hermann
Dave Reynolds


Michael Schneider and Kai Mainzer (FZI), in November 2009
