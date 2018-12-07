package net.sansa_stack.rdf.spark

import net.sansa_stack.rdf.spark.utils.Logging
import org.apache.jena.graph.{ Node, Triple }
import org.apache.spark.graphx.VertexRDD
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

package object stats {

  implicit class StatsCriteria(triples: RDD[Triple]) extends Logging {
    @transient val spark: SparkSession = SparkSession.builder().getOrCreate()

    /**
     * Compute distributed RDF dataset statistics.
     * @return VoID description of the given dataset
     */
    def stats: RDD[String] =
      RDFStatistics.run(triples)

    /**
     * <b>1. Used Classes Criterion </b> <br>
     * Creates an RDD of classes are in use by instances of the analyzed dataset.
     * As an example of such a triple that will be accepted by
     * the filter is `sda:Gezim rdf:type distLODStats:Developer`.
     * <b>Filter rule</b> : `?p=rdf:type && isIRI(?o)`
     * <b>Action</b> : `S += ?o`
     * @return RDD of classes/instances
     */
    def statsUsedClasses(): RDD[Node] =
      Used_Classes(triples, spark).Filter()

    /**
     * <b>2. Class Usage Count Criterion </b> <br>
     * Count the usage of respective classes of a datase,
     * the filter rule that is used to analyze a triple is the
     * same as in the first criterion.
     * As an action a map is being created having class IRIs as
     * identifier and its respective usage count as value.
     * If a triple is conform to the filter rule the respective
     * value will be increased by one.
     * <b>Filter rule</b> : `?p=rdf:type && isIRI(?o)`
     * <b>Action</b> : `M[?o]++ `
     * @return RDD of classes used in the dataset and their frequencies.
     */
    def statsClassUsageCount(): RDD[(Node, Int)] =
      Used_Classes(triples, spark).Action()

    /**
     * <b>3. Classes Defined Criterion </b> <br>
     * Gets a set of classes that are defined within a
     * dataset this criterion is being used.
     * Usually in RDF/S and OWL a class can be defined by a triple
     * using the predicate `rdf:type` and either `rdfs:Class` or
     * `owl:Class` as object.
     * The filter rule illustrates the condition used to analyze the triple.
     * If the triple is accepted by the rule, the IRI used as subject is added to the set of classes.
     * <b>Filter rule</b> : `?p=rdf:type && isIRI(?s) &&(?o=rdfs:Class||?o=owl:Class)`
     * <b>Action</b> : `S += ?s `
     * @return RDD of classes defined in the dataset.
     */
    def statsClassesDefined(): RDD[Node] =
      Classes_Defined(triples, spark).Action()

    /**
     *  <b>4. Class hierarchy depth criterion </b> <br>
     *  @return the depth of the graph
     */
    def classHierarchyDepth(): RDD[(Node, Int)] =
      RDFStatistics.ClassHierarchyDepth(triples)
    /**
     * <b>5. Property Usage Criterion </b> <br>
     * Count the usage of properties within triples.
     * Therefore an RDD will be created containing all property
     * IRI's as identifier.
     * Afterwards, their frequencies will be computed.
     * <b>Filter rule</b> : `none`
     * <b>Action</b> : `M[?p]++ `
     * @return RDD of predicates used in the dataset and their frequencies.
     */
    def statsPropertyUsage(): RDD[(Node, Int)] =
      PropertyUsage(triples, spark).Action()

    /**
     * <b>6. Property usage distinct per subject  </b> <br>
     * Count the usage of properties within triples based on subjects.
     * <b>Filter rule</b> : `none`
     * <b>Action</b> : `M[?s] += ?p `
     * @return RDD of predicates used in the dataset and their frequencies.
     */
    def statsPropertyUsageDistinctPerSubject(): RDD[(Iterable[Triple], Int)] =
      RDFStatistics.PropertyUsageDistinctPerSubject(triples)

    /**
     * <b>7. Property usage distinct per object   </b> <br>
     * Count the usage of properties within triples based on objects.
     * <b>Filter rule</b> : `none`
     * <b>Action</b> : `M[?o] += ?p `
     * @return RDD of predicates used in the dataset and their frequencies.
     */
    def statsPropertyUsageDistinctPerObject(): RDD[(Iterable[Triple], Int)] =
      RDFStatistics.PropertyUsageDistinctPerObject(triples)

    /**
     *  12. Property hierarchy depth criterion
     *
     *  @return the depth of the graph
     */
    def PropertyHierarchyDepth(): RDD[(Node, Int)] =
      RDFStatistics.PropertyHierarchyDepth(triples)

    /**
     * <b>16. Distinct entities </b> <br>
     * Count distinct entities of a dataset by filtering out all IRIs.
     * <b>Filter rule</b> : `S+=iris({?s,?p,?o})`
     * <b>Action</b> : `S`
     * @return RDD of distinct entities in the dataset.
     */
    def statsDistinctEntities(): RDD[Node] =
      DistinctEntities(triples, spark).Action()

    /**
     * * 17. Literals criterion
     *
     * @return number of triples that are referencing literals to subjects.
     */
    def statsLiterals(): RDD[Triple] =
      RDFStatistics.Literals(triples)

    /**
     * 18. Blanks as subject criterion
     *
     * @return number of triples where blanknodes are used as subjects.
     */
    def statsBlanksAsSubject(): RDD[Triple] =
      RDFStatistics.BlanksAsSubject(triples)

    /**
     * 19. Blanks as object criterion
     *
     * @return number of triples where blanknodes are used as objects.
     */
    def statsBlanksAsObject(): RDD[Triple] =
      RDFStatistics.BlanksAsObject(triples)

    /**
     * 20. Datatypes criterion
     *
     * @return histogram of types used for literals.
     */
    def statsDatatypes(): RDD[(String, Int)] =
      RDFStatistics.Datatypes(triples)

    /**
     * 21. Languages criterion
     *
     * @return histogram of languages used for literals.
     */
    def statsLanguages(): RDD[(String, Int)] =
      RDFStatistics.Languages(triples)

    /**
     * 22. Average typed string length criterion.
     *
     * @return the average typed string length used throughout the RDF graph.
     */
    def statsAvgTypedStringLength(): Double =
      RDFStatistics.AvgTypedStringLength(triples)

    /**
     * 23. Average untyped string length criterion.
     *
     * @return the average untyped string length used throughout the RDF graph.
     */
    def statsAvgUntypedStringLength(): Double =
      RDFStatistics.AvgUntypedStringLength(triples)

    /**
     * 24. Typed subjects criterion.
     *
     * @return list of typed subjects.
     */
    def statsTypedSubjects(): RDD[Node] =
      RDFStatistics.TypedSubjects(triples)

    /**
     * 24. Labeled subjects criterion.
     *
     * @return list of labeled subjects.
     */
    def statsLabeledSubjects(): RDD[Node] =
      RDFStatistics.LabeledSubjects(triples)

    /**
     * 25. SameAs criterion.
     *
     * @return list of triples with owl#sameAs as predicate
     */
    def statsSameAs(): RDD[Triple] =
      RDFStatistics.SameAs(triples)

    /**
     * 26. Links criterion.
     *
     * @return list of namespaces and their frequentcies.
     */
    def statsLinks(): RDD[(String, String, Int)] =
      RDFStatistics.Links(triples)

    /**
     * 28.Maximum per property {int,float,time} criterion
     *
     * @return entities with their maximum values on the graph
     */
    def statsMaxPerProperty(): RDD[(Node, Node)] =
      RDFStatistics.MaxPerProperty(triples)

    /**
     * 29. Average per property {int,float,time} criterion
     *
     * @return entities with their average values on the graph
     */
    def statsAvgPerProperty(): RDD[(Node, Double)] =
      RDFStatistics.AvgPerProperty(triples)

    /**
     * <b>30. Subject vocabularies </b> <br>
     * Compute subject vocabularies/namespaces used through the dataset.
     * <b>Filter rule</b> : `ns=ns(?s)`
     * <b>Action</b> : `M[ns]++`
     * @return RDD of distinct subject vocabularies used in the dataset and their frequencies.
     */
    def statsSubjectVocabularies(): RDD[(String, Int)] =
      SPO_Vocabularies(triples, spark).SubjectVocabulariesPostProc()

    /**
     * <b>31. Predicate vocabularies </b> <br>
     * Compute predicate vocabularies/namespaces used through the dataset.
     * <b>Filter rule</b> : `ns=ns(?p)`
     * <b>Action</b> : `M[ns]++`
     * @return RDD of distinct predicate vocabularies used in the dataset and their frequencies.
     */
    def statsPredicateVocabularies(): RDD[(String, Int)] =
      SPO_Vocabularies(triples, spark).PredicateVocabulariesPostProc()

    /**
     * <b>32. Object vocabularies </b> <br>
     * Compute object vocabularies/namespaces used through the dataset.
     * <b>Filter rule</b> : `ns=ns(?o)`
     * <b>Action</b> : `M[ns]++`
     * @return RDD of distinct object vocabularies used in the dataset and their frequencies.
     */
    def statsObjectVocabularies(): RDD[(String, Int)] =
      SPO_Vocabularies(triples, spark).ObjectVocabulariesPostProc()

    /**
     * <b>Distinct Subjects</b> <br>
     * Count distinct subject within triples.
     * <b>Filter rule</b> : `isURI(?s)`
     * <b>Action</b> : `M[?s]++`
     * @return RDD of subjects used in the dataset.
     */
    def statsDistinctSubjects(): RDD[Node] =
      DistinctSubjects(triples, spark).Action()

    /**
     * <b>Distinct Objects</b> <br>
     * Count distinct objects within triples.
     * <b>Filter rule</b> : `isURI(?o)`
     * <b>Action</b> : `M[?o]++`
     * @return RDD of objects used in the dataset.
     */
    def statsDistinctObjects(): RDD[Node] =
      DistinctObjects(triples, spark).Action()

    /**
     * <b>Properties Defined</b> <br>
     * Count the defined properties within triples.
     * <b>Filter rule</b> : `?p=rdf:type &&  (?o=owl:ObjectProperty ||
     *  ?o=rdf:Property)&& !isIRI(?s)`
     * <b>Action</b> : `M[?p]++`
     * @return RDD of predicates defined in the dataset.
     */
    def statsPropertiesDefined(): RDD[Node] =
      PropertiesDefined(triples, spark).Action()

  }

  implicit class StatsCriteriaVoidify(stats: RDD[String]) extends Logging {

    /**
     * Voidify RDF dataset based on the Vocabulary of Interlinked Datasets (VoID) [[https://www.w3.org/TR/void/]]
     *
     * @param source name of the Dataset:source--usualy the file's name
     * @param output the directory to save RDF dataset summary
     */
    def voidify(source: String, output: String): Unit =
      RDFStatistics.voidify(stats, source, output)

    /**
     * Prints the Voidiy version of the given RDF dataset
     *
     * @param source name of the Dataset:source--usualy the file's name
     */
    def print(source: String): Unit =
      RDFStatistics.print(stats, source)
  }
}
