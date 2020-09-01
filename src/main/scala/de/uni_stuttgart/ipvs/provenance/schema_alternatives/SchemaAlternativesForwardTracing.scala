package de.uni_stuttgart.ipvs.provenance.schema_alternatives

import de.uni_stuttgart.ipvs.provenance.nested_why_not.Constants
import org.apache.spark.sql.catalyst.expressions.aggregate.{AggregateExpression, CollectList, CollectSet}
import org.apache.spark.sql.catalyst.expressions.{Alias, Attribute, AttributeReference, BinaryExpression, Cast, CreateNamedStruct, Expression, GetStructField, IsNotNull, Literal}
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan

import scala.collection.mutable

object SchemaAlternativesForwardTracing{
  def apply(inputWhyNotQuestion: PrimarySchemaSubsetTree, inputPlan: LogicalPlan,
            modificationExpressions: Seq[Expression]) = {
    new SchemaAlternativesForwardTracing(inputWhyNotQuestion, inputPlan, modificationExpressions)
  }
}

class SchemaAlternativesForwardTracing(inputWhyNotQuestion: PrimarySchemaSubsetTree, inputPlan: LogicalPlan, modificationExpressions: Seq[Expression]){

  var outputTree = PrimarySchemaSubsetTree(inputWhyNotQuestion.id)
  var currentInputNode = inputWhyNotQuestion.getRootNode
  var currentBacktracedNode: SchemaNode = null


  var currentOutputNode = outputTree.getRootNode
  var deepestNode = inputWhyNotQuestion.getRootNode

  var isDescendantOfAlias = false
  var isGeneratorExpression = false

  initialize()

  def initialize(): Unit = {
    for (alternative <- inputWhyNotQuestion.alternatives){
      val newAlternative = SchemaSubsetTree(true, alternative.id)
      outputTree.alternatives += newAlternative
      outputTree.getRootNode.alternatives += newAlternative.rootNode
    }
  }

  def getOutputWhyNotQuestion(): PrimarySchemaSubsetTree = {
    outputTree
  }

  def forwardTraceExpressions(): this.type = {
    for (expression <- modificationExpressions){
      forwardTraceExpression(expression)
      assert(currentInputNode == inputWhyNotQuestion.rootNode)
      assert(currentOutputNode == outputTree.rootNode)
    }
    this
  }


  def isAggregateExpression(expression: Expression): Boolean = {
    var isAggregate = false
    for (child <- expression.children) {
      isAggregate |= isAggregateExpression(child)
    }
    isAggregate
  }

  def forwardTraceConstraintsOnAggregatedValues(outputWhyNotQuestion: SchemaSubsetTree): this.type = {
    for (expression <- modificationExpressions){
      currentOutputNode = outputTree.getRootNode
      currentBacktracedNode = outputWhyNotQuestion.rootNode
      forwardTraceConstraints(expression)
    }
    this
  }

  def forwardTraceConstraints(expression: Expression) = {
    expression match {
      case a: Alias => {
        currentOutputNode = currentOutputNode.getPrimaryChild(a.name).get
        currentBacktracedNode = currentBacktracedNode.getChild(a.name).get
        currentOutputNode.constraint = currentBacktracedNode.constraint
        copyConstraints()
      }
      case _ => {}
    }
  }



  def forwardTraceAggregateExpression(ag: AggregateExpression): Unit = {
    ag.aggregateFunction match {
      case nesting @ ( _:CollectList | _:CollectSet) => {
          currentOutputNode = createOutputNodes("element")
          forwardTraceExpression(ag.aggregateFunction.children.head)
          currentOutputNode = currentOutputNode.getParent()
        }
      case _ => {
        forwardTraceExpression(ag.aggregateFunction.children.head)
      }
    }

  }

  def forwardTraceExpression(expression: Expression): Unit = {
    expression match {

      case a: Alias => {
        forwardTraceAlias(a)
      }
      case cns: CreateNamedStruct => {
        forwardTraceNamedStruct(cns)
      }
      case a: AttributeReference => {
        forwardTraceAttribute(a) // an attribute reference is also an attribute, thus no special case needed
      }
      case gs: GetStructField => {
        forwardTraceStructField(gs)
      }
      case l: Literal => {
        forwardTraceLiteral(l)
      }

      case ag: AggregateExpression => {
        forwardTraceAggregateExpression(ag)
      }
    }
  }

  def forwardTraceAlias(a: Alias) : Unit = {
    val outputNode = createOutputNodes(a.name)
    currentOutputNode = outputNode
    isDescendantOfAlias = true
    forwardTraceExpression(a.child)
    currentOutputNode = currentOutputNode.getParent()
  }

  def createOutputNodes(name: String): PrimarySchemaNode = {
    val outputNode = PrimarySchemaNode(name, Constraint(""), currentOutputNode)
    currentOutputNode.addChild(outputNode)
    for ((alternative, id) <- currentOutputNode.alternatives zip outputTree.alternatives.map(alternative => alternative.id)) {
      val alternativeNodeName = if(name == "element") "element" else Constants.getAlternativeFieldName(name, 0, id)
      val alternativeNode = SchemaNode(alternativeNodeName, Constraint(""), alternative)
      alternative.addChild(alternativeNode)
      outputNode.addAlternative(alternativeNode)
    }
    outputNode
  }

  def createOutputNodes(inputNode: PrimarySchemaNode): PrimarySchemaNode = {
    val outputNode = PrimarySchemaNode(inputNode.name, Constraint(""), currentOutputNode)
    currentOutputNode.addChild(outputNode)
    for ((inputAlternative, outputAlternativeParent) <- currentInputNode.alternatives zip currentOutputNode.alternatives) {
      val alternativeNodeName = inputAlternative.name
      val alternativeNode = SchemaNode(alternativeNodeName, Constraint(""), outputAlternativeParent)
      outputAlternativeParent.addChild(alternativeNode)
      outputNode.addAlternative(alternativeNode)
    }
    outputNode
  }

  def copyConstraints(): Unit = {
    for(alternative <- currentOutputNode.getAllAlternatives()){
      alternative.constraint = currentOutputNode.constraint
    }
  }

  def copyChildren(): Unit = {
    for (child <- currentInputNode.getChildren){
      child.deepCopyPrimary(currentOutputNode)
    }
  }

  def forwardTraceAttribute(reference: AttributeReference): Unit = {
    currentInputNode = currentInputNode.getPrimaryChild(reference.name).get
    if(!isDescendantOfAlias){
      currentOutputNode = createOutputNodes(currentInputNode)
    }
    if(isGeneratorExpression){
      //move on to the nested element node
      currentInputNode = currentInputNode.getPrimaryChild("element").get
    }
    copyConstraints()
    copyChildren()
    if(isGeneratorExpression){
      currentInputNode = currentInputNode.getParent()
    }
    currentInputNode = currentInputNode.getParent()
    if(!isDescendantOfAlias){
      currentOutputNode = currentOutputNode.getParent()
    }
    isDescendantOfAlias = false
  }

  def forwardTraceStructField(field: GetStructField): Unit = {
    val inputNode = currentInputNode
    forwardTraceStructFieldInternal(field)
    if(!isDescendantOfAlias){
      currentOutputNode = createOutputNodes(currentInputNode)
    }
    if(isGeneratorExpression){
      //move on to the nested element node
      currentInputNode = currentInputNode.getPrimaryChild("element").get
    }
    copyConstraints()
    copyChildren()
    if(isGeneratorExpression){
      currentInputNode = currentInputNode.getParent()
    }
    currentInputNode = inputNode
    if(!isDescendantOfAlias){
      currentOutputNode = currentOutputNode.getParent()
    }
    isDescendantOfAlias = false
  }

  def forwardTraceStructFieldInternal(field: GetStructField): Unit = {
    field.child match {
      case gs: GetStructField => {
        forwardTraceStructFieldInternal(gs)
      }
      case ar: AttributeReference => {
        currentInputNode = inputWhyNotQuestion.getRootNode.getPrimaryChild(ar.name).get
      }
    }
    currentInputNode = currentInputNode.getPrimaryChild(field.name.get).get
  }

  def forwardTraceNamedStruct(struct: CreateNamedStruct): Unit = {
    val alias = isDescendantOfAlias
    isDescendantOfAlias = false
    if(!alias){
      //This case should not happen
      currentOutputNode = createOutputNodes(struct.nodeName)
    }
    for (List(name, expression) <- struct.children.grouped(2)){
      val fieldName = name.asInstanceOf[Literal].value.toString
      currentOutputNode = createOutputNodes(fieldName)
      isDescendantOfAlias = true
      forwardTraceExpression(expression)
      currentOutputNode = currentOutputNode.getParent()
    }
  }

  def forwardTraceLiteral(literal: Literal): Unit = {
    if(!isDescendantOfAlias){
      //This case should not happen
      createOutputNodes(literal.nodeName)
    }
    isDescendantOfAlias = false
  }

  def forwardTraceGenerator(inputExpressions: Seq[Expression], outputExpressions: Seq[Attribute]): PrimarySchemaSubsetTree = {
    //We only consider the first elements here
    val inputExpression = inputExpressions.head
    val outputExpression = outputExpressions.head
    isDescendantOfAlias = true
    isGeneratorExpression = true
    outputTree = inputWhyNotQuestion
    currentOutputNode = outputTree.getRootNode
    currentOutputNode = createOutputNodes(outputExpression.name)
    forwardTraceExpression(inputExpression)
    outputTree
  }
}
