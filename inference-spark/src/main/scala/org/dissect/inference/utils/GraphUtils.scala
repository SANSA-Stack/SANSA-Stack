package org.dissect.inference.utils

import java.io.{ByteArrayOutputStream, File, FileOutputStream, FileWriter}
import java.nio.charset.StandardCharsets
import java.nio.file.{Files, Paths}

import com.itextpdf.text.PageSize
import org.apache.jena.graph.Node
import org.apache.jena.reasoner.rulesys.Rule
import org.apache.jena.shared.PrefixMapping
import org.dissect.inference.utils.graph.{EdgeEquivalenceComparator, LabeledEdge, NodeEquivalenceComparator}
import org.gephi.graph.api.GraphController
import org.gephi.io.exporter.api.ExportController
import org.gephi.io.exporter.preview.PDFExporter
import org.gephi.io.importer.api.{EdgeDirectionDefault, ImportController}
import org.gephi.io.processor.plugin.DefaultProcessor
import org.gephi.layout.plugin.force.StepDisplacement
import org.gephi.layout.plugin.force.yifanHu.YifanHuLayout
import org.gephi.preview.api.{Item, PreviewController, PreviewProperty}
import org.gephi.preview.types.EdgeColor
import org.gephi.project.api.ProjectController
import org.jgrapht.DirectedGraph
import org.jgrapht.experimental.equivalence.EquivalenceComparator
import org.jgrapht.experimental.isomorphism.AdaptiveIsomorphismInspectorFactory
import org.jgrapht.ext._
import org.jgrapht.graph.{DefaultDirectedGraph, DefaultEdge}
import org.openide.util.Lookup

import scala.collection.JavaConversions._
import scalax.collection.GraphEdge.DiEdge
import scalax.collection.edge.LDiEdge

/**
  * @author Lorenz Buehmann
  *         created on 1/23/16
  */
object GraphUtils {



  def areIsomorphic(graph1: scalax.collection.mutable.Graph[Node, LDiEdge], graph2: scalax.collection.mutable.Graph[Node, LDiEdge]): Boolean = {
    val g1 = asJGraphtGraph(graph1)
    val g2 = asJGraphtGraph(graph2)

    val c1 = new NodeEquivalenceComparator()
    val c2 = new EdgeEquivalenceComparator()

    val isoDetector = AdaptiveIsomorphismInspectorFactory.createIsomorphismInspector(
      g1, g2, c1, c2)

    isoDetector.isIsomorphic
  }

  /**
    * Converts a 'Graph for Scala' graph to a JGraphT graph.
    *
    * @param graph the 'Graph for Scala' graph
    * @return the JGraphT graph
    */
  def asJGraphtGraph(graph: scalax.collection.mutable.Graph[Node, LDiEdge]): DirectedGraph[Node, LabeledEdge] = {
    val g: DirectedGraph[Node, LabeledEdge] = new DefaultDirectedGraph[Node, LabeledEdge](classOf[LabeledEdge])

    val edges = graph.edges.toList

    edges.foreach { e =>
      val nodes = e.nodes.toList
      val source = nodes(0)
      val target = nodes(1)
      g.addVertex(source)
      g.addVertex(target)

      // check if there is an edge e1 such that l(e)=source(e1) || l(e)=target(e2)
      // and if so set l(e)=l(e1)+"out"("in")
      var label = e.label.toString
      edges.foreach { e1 =>
        val nodes = e1.nodes.toList
        val source = nodes(0)
        val target = nodes(1)

        if(source.value.toString().equals(label)) {
          label = e1.label.toString + "_in"
        } else if(target.value.toString().equals(label)) {
          label = e1.label.toString + "_out"
        }
      }
      g.addEdge(source, target, new LabeledEdge(label))
    }
    g
  }

  implicit class ClassRuleDependencyGraphExporter(val graph: scalax.collection.mutable.Graph[Rule, DiEdge]) {
    /**
      * Export the rule dependency graph to GraphML format.
      *
      * @param filename the target file
      */
    def export(filename: String) = {

      val g: DirectedGraph[Rule, DefaultEdge] = new DefaultDirectedGraph[Rule, DefaultEdge](classOf[DefaultEdge])

      val edges = graph.edges.toList

      edges.foreach { e =>
        val nodes = e.nodes.toList
        val source = nodes(0)
        val target = nodes(1)
        g.addVertex(source)
        g.addVertex(target)
        g.addEdge(source, target, new DefaultEdge())
      }

      // In order to be able to export edge and node labels and IDs,
      // we must implement providers for them
      val vertexIDProvider = new VertexNameProvider[Rule]() {
        override def getVertexName(v: Rule): String = v.getName
      }

      val vertexNameProvider = new VertexNameProvider[Rule]() {
        override def getVertexName(v: Rule): String = v.getName
      }

      val edgeIDProvider = new EdgeNameProvider[LabeledEdge]() {
        override def getEdgeName(edge: LabeledEdge): String = {
          g.getEdgeSource(edge) + " > " + g.getEdgeTarget(edge)
        }
      }

      val edgeLabelProvider = new EdgeNameProvider[LabeledEdge]() {
        override def getEdgeName(e: LabeledEdge): String = e.label
      }

//      val exporter = new GraphMLExporter[String,LabeledEdge](
//        vertexIDProvider, vertexNameProvider, edgeIDProvider,edgeLabelProvider)

      val exporter = new GraphMLExporter[Rule, DefaultEdge](
        new IntegerNameProvider[Rule],
        vertexNameProvider,
        new IntegerEdgeNameProvider[DefaultEdge],
        null)

      val fw = new FileWriter(filename)

      exporter.export(fw, g)
    }
  }

  implicit class ClassRuleTriplePatternGraphExporter(val graph: scalax.collection.mutable.Graph[Node, LDiEdge]) {


//    def export2(filename: String) = {
//
//      // Gephi
//      //Init a project - and therefore a workspace
//      val pc = Lookup.getDefault().lookup(classOf[ProjectController]);
//      pc.newProject();
//      val workspace = pc.getCurrentWorkspace();
//
//      //Get controllers and models
//      val importController = Lookup.getDefault().lookup(classOf[ImportController]);
//
//
//      //See if graph is well imported
//      val graphModel = Lookup.getDefault().lookup(classOf[GraphController]).getModel;
//      val g = graphModel.getDirectedGraph();
//
//      val edges = graph.edges.toList
//
//      edges.foreach { e =>
//        val nodes = e.nodes.toList
//        val source = graphModel.factory().newNode(nodes(0).toString())
//        val target = graphModel.factory().newNode(nodes(1).toString())
//        if(!g.contains(source))
//          g.addNode(source)
//        if(!g.contains(target))
//          g.addNode(target)
//        val edge = graphModel.factory().newEdge(source, target, 1.0f, true)
//        g.addEdge(edge)
//      }
//
//
//      //Run YifanHuLayout for 100 passes - The layout always takes the current visible view
//      val layout = new YifanHuLayout(null, new StepDisplacement(1f));
//      layout.setGraphModel(graphModel);
//      layout.resetPropertiesValues();
//      layout.setOptimalDistance(200f);
//
//      layout.initAlgo();
//      for (i <- 0 to 100 if layout.canAlgo()) {
//        layout.goAlgo();
//      }
//      layout.endAlgo();
//
//      val model = Lookup.getDefault().lookup(classOf[PreviewController]).getModel();
//      model.getProperties().putValue(PreviewProperty.SHOW_NODE_LABELS, true);
//      model.getProperties().putValue(PreviewProperty.EDGE_CURVED, false);
//      model.getProperties().putValue(PreviewProperty.EDGE_COLOR, new EdgeColor(java.awt.Color.GRAY));
//      model.getProperties().putValue(PreviewProperty.EDGE_THICKNESS, 0.1f);
//      model.getProperties().putValue(PreviewProperty.NODE_LABEL_FONT, model.getProperties().getFontValue(PreviewProperty.NODE_LABEL_FONT).deriveFont(8));
//      //      model.getProperties.putValue(Item.NODE_LABEL, "vertex_label")
//
//      for (item <- model.getItems(Item.NODE_LABEL)) {
//        println(item)
//      }
//
//
//      //Export full graph
//      val ec = Lookup.getDefault().lookup(classOf[ExportController]);
//      //      ec.exportFile(new File("io_gexf.gexf"));
//
//      //PDF Exporter config and export to Byte array
//      val pdfExporter = ec.getExporter("pdf").asInstanceOf[PDFExporter];
//      pdfExporter.setPageSize(PageSize.A0);
//      pdfExporter.setWorkspace(workspace);
//      val baos = new ByteArrayOutputStream();
//      ec.exportStream(baos, pdfExporter);
//      new FileOutputStream(filename + ".pdf").write(baos.toByteArray())
//    }

    /**
      * Export the rule dependency graph to GraphML format.
      *
      * @param filename the target file
      */
    def export(filename: String) = {

      val g: DirectedGraph[Node, LabeledEdge] = new DefaultDirectedGraph[Node, LabeledEdge](classOf[LabeledEdge])

      val edges = graph.edges.toList

      edges.foreach { e =>
        val nodes = e.nodes.toList
        val source = nodes(0)
        val target = nodes(1)
        g.addVertex(source)
        g.addVertex(target)
        g.addEdge(source, target, new LabeledEdge(PrefixMapping.Standard.shortForm(e.label.toString)))
      }

      // In order to be able to export edge and node labels and IDs,
      // we must implement providers for them
      val vertexIDProvider = new VertexNameProvider[Node]() {
        override def getVertexName(v: Node): String = v.toString(PrefixMapping.Standard)
      }

      val vertexNameProvider = new VertexNameProvider[Node]() {
        override def getVertexName(v: Node): String = v.toString(PrefixMapping.Standard)
      }

      val edgeIDProvider = new EdgeNameProvider[LabeledEdge]() {
        override def getEdgeName(edge: LabeledEdge): String = {
          g.getEdgeSource(edge).toString(PrefixMapping.Standard) + " > " + edge.label + " > " + g.getEdgeTarget(edge).toString(PrefixMapping.Standard)
        }
      }

      val edgeLabelProvider = new EdgeNameProvider[LabeledEdge]() {
        override def getEdgeName(e: LabeledEdge): String = e.label
      }

      //      val exporter = new GraphMLExporter[String,LabeledEdge](
      //        vertexIDProvider, vertexNameProvider, edgeIDProvider,edgeLabelProvider)

      val exporter = new GraphMLExporter[Node, LabeledEdge](
        new IntegerNameProvider[Node],
        vertexNameProvider,
        new IntegerEdgeNameProvider[LabeledEdge],
        edgeLabelProvider)

      val fw = new FileWriter(filename)

      exporter.export(fw, g)

      val path = Paths.get(filename)
      val charset = StandardCharsets.UTF_8

      var content = new String(Files.readAllBytes(path), charset)
      content = content.replaceAll("vertex_label", "node_label")
      Files.write(path, content.getBytes(charset))

      // Gephi
      //Init a project - and therefore a workspace
      val pc = Lookup.getDefault.lookup(classOf[ProjectController])
      pc.newProject()
      val workspace = pc.getCurrentWorkspace

      //Get controllers and models
      val importController = Lookup.getDefault.lookup(classOf[ImportController])

      //Import file
      val file = new File(filename)
      var container = importController.importFile(file)
      container.getLoader.setEdgeDefault(EdgeDirectionDefault.DIRECTED);   //Force DIRECTED
      //        container.setAllowAutoNode(false);  //Don't create missing nodes

      //Append imported data to GraphAPI
      importController.process(container, new DefaultProcessor(), workspace)

      //See if graph is well imported
      val graphModel = Lookup.getDefault.lookup(classOf[GraphController]).getGraphModel
      val diGraph = graphModel.getDirectedGraph()

      for(node <- diGraph.getNodes) {
        node.setLabel(node.getAttribute("node_label").toString)
      }

      for(edge <- diGraph.getEdges) {
        edge.setLabel(edge.getAttribute("edge_label").toString)
      }

      //Run YifanHuLayout for 100 passes - The layout always takes the current visible view
      val layout = new YifanHuLayout(null, new StepDisplacement(1f))
      layout.setGraphModel(graphModel)
      layout.resetPropertiesValues()
      layout.setOptimalDistance(200f)

      //      layout.initAlgo();
//      for (i <- 0 to 100 if layout.canAlgo()) {
//        layout.goAlgo();
//      }
//      layout.endAlgo();

      val model = Lookup.getDefault.lookup(classOf[PreviewController]).getModel()
      model.getProperties.putValue(PreviewProperty.SHOW_NODE_LABELS, true)
      model.getProperties.putValue(PreviewProperty.EDGE_CURVED, false)
      model.getProperties.putValue(PreviewProperty.EDGE_COLOR, new EdgeColor(java.awt.Color.GRAY))
      model.getProperties.putValue(PreviewProperty.EDGE_THICKNESS, 0.1f)
      model.getProperties.putValue(PreviewProperty.NODE_LABEL_FONT, model.getProperties.getFontValue(PreviewProperty.NODE_LABEL_FONT).deriveFont(8))

      model.getProperties.putValue(Item.NODE_LABEL, "Vertex Label")
      model.getProperties.putValue(PreviewProperty.SHOW_EDGE_LABELS, true)
      model.getProperties.putValue(PreviewProperty.NODE_LABEL_SHOW_BOX, false)

      for (item <- model.getItems(Item.NODE_LABEL)) {
        println(item)
      }


  //Export full graph
      val ec = Lookup.getDefault.lookup(classOf[ExportController])
      //      ec.exportFile(new File("io_gexf.gexf"));

      //PDF Exporter config and export to Byte array
      val pdfExporter = ec.getExporter("pdf").asInstanceOf[PDFExporter]
      pdfExporter.setPageSize(PageSize.A0)
      pdfExporter.setWorkspace(workspace)
      val baos = new ByteArrayOutputStream()
      ec.exportStream(baos, pdfExporter)
      new FileOutputStream(filename + ".pdf").write(baos.toByteArray)
    }
  }
}
