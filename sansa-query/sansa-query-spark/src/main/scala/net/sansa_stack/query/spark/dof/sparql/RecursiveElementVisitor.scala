package net.sansa_stack.query.spark.dof.sparql

import net.sansa_stack.query.spark.dof.bindings.Result
import net.sansa_stack.query.spark.dof.node.Pattern
import net.sansa_stack.query.spark.dof.tensor.Tensor
import org.apache.jena.sparql.syntax._

import scala.jdk.CollectionConverters._
import scala.reflect.ClassTag

class RecursiveElementVisitor[R, N: ClassTag, T, A](model: Tensor[R, N, T, A]) extends ElementVisitorBase {

  def startElement(el: ElementTriplesBlock): Unit = {}
  def endElement(el: ElementTriplesBlock): Unit = {}

  def startElement(el: ElementDataset): Unit = {}
  def endElement(el: ElementDataset): Unit = {}

  def startElement(el: ElementFilter): Unit = {}
  def endElement(el: ElementFilter): Unit = {}

  def startElement(el: ElementAssign): Unit = {}
  def endElement(el: ElementAssign): Unit = {}

  def startElement(el: ElementBind): Unit = {}
  def endElement(el: ElementBind): Unit = {}

  def startElement(el: ElementData): Unit = {}
  def endElement(el: ElementData): Unit = {}

  def startElement(el: ElementUnion): Unit = {}
  def endElement(el: ElementUnion): Unit = {}
  def startSubElement(el: ElementUnion, subElt: Element): Unit = {}
  def endSubElement(el: ElementUnion, subElt: Element): Unit = {}

  def startElement(el: ElementGroup): Unit = {}
  def endElement(el: ElementGroup): Unit = {}
  def startSubElement(el: ElementGroup, subElt: Element): Unit = {}
  def endSubElement(el: ElementGroup, subElt: Element): Unit = {}

  def startElement(el: ElementOptional): Unit = {}
  def endElement(el: ElementOptional): Unit = {}

  def startElement(el: ElementNamedGraph): Unit = {}
  def endElement(el: ElementNamedGraph): Unit = {}

  def startElement(el: ElementService): Unit = {}
  def endElement(el: ElementService): Unit = {}

  def startElement(el: ElementExists): Unit = {}
  def endElement(el: ElementExists): Unit = {}

  def startElement(el: ElementNotExists): Unit = {}
  def endElement(el: ElementNotExists): Unit = {}

  def startElement(el: ElementMinus): Unit = {}
  def endElement(el: ElementMinus): Unit = {}

  def endElement(el: ElementSubQuery): Unit = {}
  def startElement(el: ElementSubQuery): Unit = {}

  def endElement(el: ElementPathBlock): Unit = {}
  def startElement(el: ElementPathBlock): Unit = {}

  private var _result: Result[A] = _
  def result: Result[A] = _result
  def result_=(value: Result[A]): Unit = _result = value

  override def visit(el: ElementPathBlock): Unit =
    {
      val constraints = Pattern.traverse(el)
      val current = new Builder(model, constraints).application

      result = model.unionResult(result, current)

      startElement(el)
      endElement(el)
    }

  override def visit(el: ElementTriplesBlock): Unit =
    {
      startElement(el)
      endElement(el)
    }

  override def visit(el: ElementDataset): Unit =
    {
      startElement(el)
      endElement(el)
    }

  override def visit(el: ElementFilter): Unit =
    {
      startElement(el)
      endElement(el)
    }

  override def visit(el: ElementAssign): Unit =
    {
      startElement(el)
      endElement(el)
    }

  override def visit(el: ElementUnion): Unit =
    {
      startElement(el)

      for (subElement <- el.getElements.asScala) {
        startSubElement(el, subElement)
        subElement.visit(this)
        endSubElement(el, subElement)
      }

      endElement(el)
    }

  override def visit(el: ElementOptional): Unit =
    {
      startElement(el)
      el.getOptionalElement.visit(this)
      endElement(el)
    }

  override def visit(el: ElementNamedGraph): Unit =
    {
      startElement(el)
      el.getElement.visit(this)
      endElement(el)
    }

  override def visit(el: ElementService): Unit =
    {
      startElement(el)
      el.getElement.visit(this)
      endElement(el)
    }

  override def visit(el: ElementExists): Unit =
    {
      startElement(el)
      el.getElement.visit(this)
      endElement(el)
    }

  override def visit(el: ElementNotExists): Unit =
    {
      startElement(el)
      el.getElement.visit(this)
      endElement(el)
    }

  override def visit(el: ElementMinus): Unit =
    {
      startElement(el)
      el.getMinusElement.visit(this)
      endElement(el)
    }

  override def visit(el: ElementSubQuery): Unit =
    {
      startElement(el)
      endElement(el)
    }
}
