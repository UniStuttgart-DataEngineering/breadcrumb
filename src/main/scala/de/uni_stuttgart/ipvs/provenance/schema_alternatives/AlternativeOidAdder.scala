package de.uni_stuttgart.ipvs.provenance.schema_alternatives

import de.uni_stuttgart.ipvs.provenance.nested_why_not.ProvenanceContext
import org.apache.spark.sql.catalyst.expressions.aggregate.AggregateExpression
import org.apache.spark.sql.catalyst.expressions.{Alias, AttributeReference, BinaryExpression, CreateNamedStruct, Expression, GetStructField, LeafExpression, UnaryExpression}

object SchemaSubsetTreeAccessAdder {
  def apply(provenanceContext: ProvenanceContext, expressions: Seq[Expression], oid: Int) = new AlternativeOidAdder(provenanceContext, expressions, oid: Int)
}

class AlternativeOidAdder(provenanceContext: ProvenanceContext, expressions: Seq[Expression], oid: Int) {

  var currentNode: PrimarySchemaNode = provenanceContext.primarySchemaAlternative.getRootNode

  def traceAttributeAccess(): Unit = {
    for (expression <- expressions) {
      traceAttributeAccess(expression)
      assert(currentNode == provenanceContext.primarySchemaAlternative.getRootNode)
    }
  }

  def traceAttributeAccessAlias(a: Alias): Unit = {
    traceAttributeAccess(a.child)
  }

  def traceAttributeAccessNamedStruct(cns: CreateNamedStruct): Unit = {
    cns.children.map{ child => traceAttributeAccess(child)}
  }


  def traceAttributeAccessAttribute(a: AttributeReference): Unit = {
    val node = currentNode.getPrimaryChild(a.name).getOrElse(return)
    for ((altNode, id) <- node.alternatives zip provenanceContext.primarySchemaAlternative.alternatives.map(_.id)){
      if (altNode.modified){
        provenanceContext.addModifiedOperatorIdtoSchemaAlternative(id, oid)
      }
    }
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

  protected def traceAttributeAccessStructFieldInternal(gs: GetStructField): Unit = {
    gs.child match {
      case a: AttributeReference => {
        traceAttributeAccessAttribute(a)
        currentNode = currentNode.getPrimaryChild(a.name).getOrElse(return)
      }
      case gs: GetStructField => {
        traceAttributeAccessStructFieldInternal(gs: GetStructField)
      }
    }
    val node = currentNode.getPrimaryChild(gs.name.get).getOrElse(return)
    if (node.modified){
      return
    }
    currentNode = node
  }

  def traceAttributeAccessStructField(gs: GetStructField): Unit = {
    val startNode = currentNode
    traceAttributeAccessStructFieldInternal(gs)
    currentNode = startNode
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

}
