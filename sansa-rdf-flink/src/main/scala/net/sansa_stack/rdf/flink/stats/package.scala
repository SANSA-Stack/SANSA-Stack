package net.sansa_stack.rdf.flink

import net.sansa_stack.rdf.flink.utils.{ Logging, StatsPrefixes }
import org.apache.flink.api.scala._
import org.apache.flink.api.scala.DataSet
import org.apache.jena.graph.{ Node, Triple }

package object stats {

  implicit class StatsCriteria(triples: DataSet[Triple]) extends Logging {

    @transient val env = ExecutionEnvironment.getExecutionEnvironment

    /**
     * Compute distributed RDF dataset statistics.
     * @return VoID description of the given dataset
     */
    def stats: DataSet[String] =
      RDFStatistics(triples, env).run()

    /**
     * <b>1. Used Classes Criterion </b> <br>
     * Creates an DataSet of classes are in use by instances of the analyzed dataset.
     * As an example of such a triple that will be accepted by
     * the filter is `sda:Gezim rdf:type distLODStats:Developer`.
     * <b>Filter rule</b> : `?p=rdf:type && isIRI(?o)`
     * <b>Action</b> : `S += ?o`
     * @return DataSet of classes/instances
     */
    def statsUsedClasses(): DataSet[Triple] =
      Used_Classes(triples, env).Filter()

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
     * @return DataSet of classes used in the dataset and their frequencies.
     */
    def statsClassUsageCount(): AggregateDataSet[(Node, Int)] =
      Used_Classes(triples, env).Action()

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
     * @return DataSet of classes defined in the dataset.
     */
    def statsClassesDefined(): DataSet[Node] =
      Classes_Defined(triples, env).Action()

    /**
     * <b>5. Property Usage Criterion </b> <br>
     * Count the usage of properties within triples.
     * Therefore an DataSet will be created containing all property
     * IRI's as identifier.
     * Afterwards, their frequencies will be computed.
     * <b>Filter rule</b> : `none`
     * <b>Action</b> : `M[?p]++ `
     * @return DataSet of predicates used in the dataset and their frequencies.
     */
    def statsPropertyUsage(): AggregateDataSet[(Node, Int)] =
      PropertyUsage(triples, env).Action()

    /**
     * <b>16. Distinct entities </b> <br>
     * Count distinct entities of a dataset by filtering out all IRIs.
     * <b>Filter rule</b> : `S+=iris({?s,?p,?o})`
     * <b>Action</b> : `S`
     * @return DataSet of distinct entities in the dataset.
     */
    def statsDistinctEntities(): DataSet[Triple] =
      DistinctEntities(triples, env).Action()

    /**
     * <b>30. Subject vocabularies </b> <br>
     * Compute subject vocabularies/namespaces used through the dataset.
     * <b>Filter rule</b> : `ns=ns(?s)`
     * <b>Action</b> : `M[ns]++`
     * @return DataSet of distinct subject vocabularies used in the dataset and their frequencies.
     */
    def statsSubjectVocabularies(): AggregateDataSet[(String, Int)] =
      SPO_Vocabularies(triples, env).SubjectVocabulariesPostProc()

    /**
     * <b>31. Predicate vocabularies </b> <br>
     * Compute predicate vocabularies/namespaces used through the dataset.
     * <b>Filter rule</b> : `ns=ns(?p)`
     * <b>Action</b> : `M[ns]++`
     * @return DataSet of distinct predicate vocabularies used in the dataset and their frequencies.
     */
    def statsPredicateVocabularies(): AggregateDataSet[(String, Int)] =
      SPO_Vocabularies(triples, env).PredicateVocabulariesPostProc()

    /**
     * <b>32. Object vocabularies </b> <br>
     * Compute object vocabularies/namespaces used through the dataset.
     * <b>Filter rule</b> : `ns=ns(?o)`
     * <b>Action</b> : `M[ns]++`
     * @return DataSet of distinct object vocabularies used in the dataset and their frequencies.
     */
    def statsObjectVocabularies(): AggregateDataSet[(String, Int)] =
      SPO_Vocabularies(triples, env).ObjectVocabulariesPostProc()

    /**
     * <b>Distinct Subjects</b> <br>
     * Count distinct subject within triples.
     * <b>Filter rule</b> : `isURI(?s)`
     * <b>Action</b> : `M[?s]++`
     * @return DataSet of subjects used in the dataset.
     */
    def statsDistinctSubjects(): DataSet[Triple] =
      DistinctSubjects(triples, env).Action()

    /**
     * <b>Distinct Objects</b> <br>
     * Count distinct objects within triples.
     * <b>Filter rule</b> : `isURI(?o)`
     * <b>Action</b> : `M[?o]++`
     * @return DataSet of objects used in the dataset.
     */
    def statsDistinctObjects(): DataSet[Triple] =
      DistinctObjects(triples, env).Action()

    /**
     * <b>Properties Defined</b> <br>
     * Count the defined properties within triples.
     * <b>Filter rule</b> : `?p=rdf:type &&  (?o=owl:ObjectProperty ||
     *  ?o=rdf:Property)&& !isIRI(?s)`
     * <b>Action</b> : `M[?p]++`
     * @return DataSet of predicates defined in the dataset.
     */
    def statsPropertiesDefined(): DataSet[Node] =
      PropertiesDefined(triples, env).Action()
  }
}
