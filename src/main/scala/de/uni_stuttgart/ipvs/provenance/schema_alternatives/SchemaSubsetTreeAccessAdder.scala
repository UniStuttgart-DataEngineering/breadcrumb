package de.uni_stuttgart.ipvs.provenance.schema_alternatives

import org.apache.spark.sql.catalyst.expressions.aggregate.AggregateExpression
import org.apache.spark.sql.catalyst.expressions.{Alias, AttributeReference, BinaryExpression, Cast, CreateNamedStruct, Expression, GetStructField, IsNotNull, LeafExpression, Literal, UnaryExpression}

object SchemaSubsetTreeAccessAdder {
  def apply(whyNotQuestion: SchemaSubsetTree, expressions: Seq[Expression]) = new SchemaSubsetTreeAccessAdder(whyNotQuestion, expressions)
}

class SchemaSubsetTreeAccessAdder(whyNotQuestion: SchemaSubsetTree, expressions: Seq[Expression]) {

  var currentNode = whyNotQuestion.rootNode

  def traceAttributeAccess(): SchemaSubsetTree = {
    for (expression <- expressions) {
      traceAttributeAccess(expression)
      assert(currentNode == whyNotQuestion.rootNode)
    }
    whyNotQuestion
  }

  def traceAttributeAccess(expression: Expression): Unit = {
    expression match {

      case a: Alias => {
        traceAttributeAccessAlias(a)
      }
      case cns: CreateNamedStruct => {
        traceAttributeAccessNamedStruct(cns)
      }
      case a: AttributeReference => {
        traceAttributeAccessAttribute(a) // an attribute reference is also an attribute, thus no special case needed
      }
      case gs: GetStructField => {
        traceAttributeAccessStructField(gs)
      }
      case ag: AggregateExpression => {
        traceAttributeAccessAggregateExpression(ag)
      }
      case _: LeafExpression => {}
      case u: UnaryExpression => {
        traceAttributeAccessUnaryExpression(u)
      }
      case b: BinaryExpression => {
        traceAttributeAccessBinaryExpression(b)
      }

    }
  }

  def traceAttributeAccessAlias(a: Alias): Unit = {
    traceAttributeAccess(a.child)
  }

  def traceAttributeAccessNamedStruct(cns: CreateNamedStruct): Unit = {
    cns.children.map{ child => traceAttributeAccess(child)}
  }

  def addChildToCurrentNode(name: String): Unit = {
    currentNode.addChild(SchemaNode(name, Constraint(""), currentNode))
  }

  def traceAttributeAccessAttribute(a: AttributeReference): Unit = {
    currentNode.getChild(a.name).getOrElse(addChildToCurrentNode(a.name))
  }

  protected def traceAttributeAccessStructFieldInternal(gs: GetStructField): Unit = {
    gs.child match {
      case a: AttributeReference => {
        traceAttributeAccessAttribute(a)
        currentNode = currentNode.getChild(a.name).get
      }
      case gs: GetStructField => {
        traceAttributeAccessStructFieldInternal(gs: GetStructField)
      }
    }
    currentNode.getChild(gs.name.get).getOrElse(addChildToCurrentNode(gs.name.get))
    currentNode = currentNode.getChild(gs.name.get).get
  }

  def traceAttributeAccessStructField(gs: GetStructField): Unit = {
    val startNode = currentNode
    traceAttributeAccessStructFieldInternal(gs)
    currentNode = startNode
  }

  def traceAttributeAccessUnaryExpression(u: UnaryExpression): Unit = {
    traceAttributeAccess(u.child)
  }
  def traceAttributeAccessBinaryExpression(b: BinaryExpression): Unit = {
    traceAttributeAccess(b.left)
    traceAttributeAccess(b.right)
  }

  def traceAttributeAccessAggregateExpression(aggregateExpression: AggregateExpression): Unit = {
    aggregateExpression.aggregateFunction.children.map {
      child => traceAttributeAccess(child)
    }
  }

}
