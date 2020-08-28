package de.uni_stuttgart.ipvs.provenance.schema_alternatives

import de.uni_stuttgart.ipvs.provenance.nested_why_not.Constants
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
      /*
      case ag: AggregateExpression => {
        //forwardTraceAggregateExpression(ag)
      }*/
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
      val alternativeNode = SchemaNode(Constants.getAlternativeFieldName(name, 0, id), Constraint(""), alternative)
      alternative.addChild(alternativeNode)
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
      currentOutputNode = createOutputNodes(reference.name)
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
      currentOutputNode = createOutputNodes(currentInputNode.name)
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
    if(!alias){
      //This case should not happen
      currentOutputNode = createOutputNodes(struct.nodeName)
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
