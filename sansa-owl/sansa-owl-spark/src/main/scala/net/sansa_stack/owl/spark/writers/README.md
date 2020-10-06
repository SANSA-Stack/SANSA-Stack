# OWL Writers

We support to write `RDD`s of OWL axioms back to the file system in a variety of formats.
This is performed like this:

```scala
[...]
import org.semanticweb.owlapi.formats.DLSyntaxDocumentFormat

import net.sansa_stack.owl.spark.owl._

val spark = SparkSession.builder().[...]
val owlAxiomRDD = [...]
val syntax = new DLSyntaxDocumentFormat

owlAxiomRDD.save("/output/dir/path/", syntax)
```

To inducate the output format we re-use the `OWLDocumentFormat` classes from the OWLAPI.
Output formats currently supported in SANSA-OWL are

- DL Syntax (`org.semanticweb.owlapi.formats.DLSyntaxDocumentFormat`)
- JSON LD (`org.semanticweb.owlapi.formats.RDFJsonDocumentFormat`)
- KRSS (`org.semanticweb.owlapi.formats.KRSSDocumentFormat`)
- KRSS 2 (`org.semanticweb.owlapi.formats.KRSS2DocumentFormat`)
- Manchester OWL Syntax (`org.semanticweb.owlapi.formats.ManchesterSyntaxDocumentFormat`)
- N3 (`org.semanticweb.owlapi.formats.N3DocumentFormat`)
- N-Quads (`org.semanticweb.owlapi.formats.NQuadsDocumentFormat`)
- N-Triples (`org.semanticweb.owlapi.formats.NTriplesDocumentFormat`)
- OBO Format (`org.semanticweb.owlapi.formats.OBODocumentFormat`)
- OWL Functional Syntax (`org.semanticweb.owlapi.formats.FunctionalSyntaxDocumentFormat`)
- OWL/XML (`org.semanticweb.owlapi.formats.OWLXMLDocumentFormat`)
- RDF JSON (`org.semanticweb.owlapi.formats.RDFJsonDocumentFormat`)
- RDF/XML (`org.semanticweb.owlapi.formats.RDFXMLDocumentFormat`)
- Trix (`org.semanticweb.owlapi.formats.TrixDocumentFormat`)
- Turtle (`org.semanticweb.owlapi.formats.TurtleDocumentFormat`)
