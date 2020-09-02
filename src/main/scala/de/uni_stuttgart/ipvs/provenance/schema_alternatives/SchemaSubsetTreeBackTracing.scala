package de.uni_stuttgart.ipvs.provenance.schema_alternatives

import org.apache.spark.sql.catalyst.expressions.aggregate.{AggregateExpression, CollectList, CollectSet}
import org.apache.spark.sql.catalyst.expressions.{Alias, Attribute, AttributeReference, CreateNamedStruct, Expression, ExtractValue, GetStructField, Literal}
import org.apache.spark.sql.types.{StructField, StructType}
import org.apache.spark.unsafe.types.UTF8String

import scala.collection.mutable

object SchemaSubsetTreeBackTracing {
  def apply(outputWhyNotQuestion: SchemaSubsetTree, inputAttributes: Seq[Attribute], outputAttributes: Seq[Attribute], modificationExpressions: Seq[Expression]) = {
    new SchemaSubsetTreeBackTracing(outputWhyNotQuestion, inputAttributes, outputAttributes, modificationExpressions)
  }
}

class SchemaSubsetTreeBackTracing(outputWhyNotQuestion: SchemaSubsetTree, inputAttributes: Seq[Attribute],
                                  outputAttributes: Seq[Attribute], modificationExpressions: Seq[Expression]) {

  var inputWhyNotQuestion = SchemaSubsetTree()

  var currentOutputNode = outputWhyNotQuestion.rootNode
  var currentInputNode = inputWhyNotQuestion.rootNode

  var directChildOfAlias = false
  var generateAccess = false
  var inside_aggregation_function = false

  val inToOutAttr = scala.collection.mutable.Map[String,String]()
  var unionAccess = false


  def backtraceExpressions(): Unit = {
    for (expression <- modificationExpressions){
      backtraceExpression(expression)
      assert(currentOutputNode == outputWhyNotQuestion.rootNode)
      assert(currentInputNode == inputWhyNotQuestion.rootNode)
    }
  }

  def backtraceExpression(expression: Expression): Boolean = {
    expression match {
      case a: Alias => {
        backtraceAlias(a)
      }
      case cns: CreateNamedStruct => {
        backtraceCreateNamedStruct(cns)
      }
      case a: Attribute => {
        backtraceAttribute(a) // an attribute reference is also an attribute, thus no special case needed
      }
      case gs: GetStructField => {
        backtraceGetStructField(gs)
      }
      case l: Literal => {
        backtraceLiteral(l)
      }
      case ag: AggregateExpression => {
        backtraceAggregateExpression(ag)
      }
    }

  }

  def backtraceGenerator(): SchemaSubsetTree = {
    directChildOfAlias = true
    generateAccess = true

    inputWhyNotQuestion = outputWhyNotQuestion.deepCopy()
    inputWhyNotQuestion.rootNode.children = inputWhyNotQuestion.rootNode.children.filterNot(node => node.name == outputAttributes.head.name)
    currentInputNode = inputWhyNotQuestion.rootNode

    val flattenedAttrName = outputAttributes.head.name
    currentOutputNode = currentOutputNode.getChild(flattenedAttrName)
      .getOrElse(SchemaNode(flattenedAttrName, currentOutputNode.constraint, currentOutputNode))

    val flattenedExpr = modificationExpressions.head
    backtraceExpression(flattenedExpr)

    /*
    // Keep the inputWhyNotQuestion has same structure as outputWhyNotQuestion
    if (currentInputNode.children.size != outputWhyNotQuestion.rootNode.children) {
      val tempOrigWhyNotQuestion = outputWhyNotQuestion.deepCopy()
      val rewrittenChildNode = tempOrigWhyNotQuestion.rootNode.getChild(flattenedAttrName).getOrElse(null)

      if (rewrittenChildNode != null) {
        tempOrigWhyNotQuestion.rootNode.removeChild(rewrittenChildNode)

        for (child <- tempOrigWhyNotQuestion.rootNode.children)
          currentInputNode.addChild(child)
      }
    }
    */

    generateAccess = false
    inputWhyNotQuestion
  }

  def backtraceUnion(): SchemaSubsetTree = {
    directChildOfAlias = true
    unionAccess = true

    /*
      Create a map: (output attr, input attr)
      Purpose: only the left child has same attribute names as output
        For other children, we use map to check which input attr corresponds to which output attr
        Later, we add the node with input attr name but copy the constraint from the corresponding node in the output tree.
     */
    val inAndOutPair = inputAttributes.zip(outputAttributes)
    for (pair <- inAndOutPair) {
      inToOutAttr.put(pair._1.name, pair._2.name)
    }
    backtraceExpressions()

    unionAccess = false
    inputWhyNotQuestion
  }

  def backtraceAggregateExpression(ag: AggregateExpression): Boolean = {
    directChildOfAlias = true
    ag.aggregateFunction match {
      case nesting @ ( _:CollectList | _:CollectSet) => {
        currentOutputNode = currentOutputNode.children.find(node => node.name == "element").getOrElse({
          currentOutputNode = currentOutputNode.parent
          return false
        })
        inside_aggregation_function = false
      }
      case _ => {
        inside_aggregation_function = true
      }
    }
    val res = backtraceExpression(ag.aggregateFunction.children.head)
    if (inside_aggregation_function) {
      inside_aggregation_function = false
    } else {
      currentOutputNode = currentOutputNode.parent
    }

    res
  }

  def backtraceAlias(alias: Alias): Boolean = {
    currentOutputNode = currentOutputNode.getChild(alias.name).getOrElse(return false)
    directChildOfAlias = true
    backtraceExpression(alias.child)
  }

  def backtraceAttribute(attribute: Attribute): Boolean = {
    if (unionAccess) {
      directChildOfAlias = true
    }

    val name = attribute.name

    if (!directChildOfAlias) {
      currentOutputNode = currentOutputNode.getChild(name).getOrElse(return false)
    }
    directChildOfAlias = false

    if (unionAccess) {
      val outputName = inToOutAttr.get(name).getOrElse(return false)
      currentOutputNode = currentOutputNode.getChild(outputName).getOrElse(return false)
    }

    //TODO: if an attribute is referenced multiple times, constraints need special handling
    var currentConstraint = Constraint("")
    if (! inside_aggregation_function){
      currentConstraint = currentOutputNode.constraint
    }
    if (currentInputNode.getChild(name).isDefined){
      currentInputNode = currentInputNode.getChild(name).get

    } else {
      currentInputNode = SchemaNode(name, currentConstraint, currentInputNode)

    }
    currentInputNode.parent.addChild(currentInputNode)

    if (generateAccess) {
      handleGenerateAccess()
    }

    if (!inside_aggregation_function){
      copyChildrenOfOutputAttributeToInputAttribute()
    }


    if (generateAccess) {
      currentInputNode = currentInputNode.parent
    }

    currentInputNode = currentInputNode.parent
    currentOutputNode = currentOutputNode.parent

    /*
    val newNode = SchemaNode(attribute.name, currentOutputNode.constraint, currentInputNode)
    currentInputNode.addChild(newNode)

    if (attribute.dataType.typeName.equals("struct") && currentInputNode.name.equals("root")) {
        backtraceStructType(attribute, attribute.dataType.asInstanceOf[StructType])
    }
    */

    true
  }

  def copyChildrenOfOutputAttributeToInputAttribute(): Unit = {
    for (child <- currentOutputNode.children) {
      val newNode = child.deepCopy(currentInputNode)
      currentInputNode.addChild(newNode)
    }
  }

  def handleGenerateAccess(): Unit = {
//    currentInputNode.constraint = Constraint("") // TODO: potentially faulty, if attribute already exists in the input schema subset
    val name = "element"
    currentOutputNode = currentOutputNode.getChild(name).getOrElse(SchemaNode(name, currentOutputNode.constraint, currentOutputNode))
    val newNode = currentOutputNode.deepCopy(currentInputNode)
    currentInputNode.addChild(newNode)

    currentOutputNode = currentOutputNode.parent
    currentInputNode = currentInputNode.getChild(name).getOrElse(null)
    assert(currentInputNode != null)
  }

  def backtraceCreateNamedStruct(createNamedStruct: CreateNamedStruct): Boolean = {
    if (!directChildOfAlias){
      currentOutputNode = currentOutputNode.getChild(createNamedStruct.prettyName).getOrElse(return false)
    }
    directChildOfAlias = false
    var valid = false
    val nameAttributePairs = createNamedStruct.names.zip(createNamedStruct.valExprs)
    for ((name, expression) <- nameAttributePairs) {
      var nestedValid = false
      name match {
        case name @ ( _:String | _:UTF8String) => {
          currentOutputNode = currentOutputNode.getChild(name.toString) match {
            case Some(child) => {
              nestedValid = true
              child
            }
            case None => {
              currentOutputNode
            }
          }
        }
        case other => {
          throw new MatchError("Unsupported name type in CreateNamedStruct: " + other.getClass.toString)
        }
      }
      if (nestedValid) {
        this.directChildOfAlias = true
        nestedValid = backtraceExpression(expression)
      }
      valid |= nestedValid
    }
    currentOutputNode = currentOutputNode.parent
    valid
  }

  def backtraceGetStructFieldInternal(field: GetStructField): Boolean = {
    field.child match {
      case gs: GetStructField => {
        backtraceGetStructFieldInternal(gs)
      }
      case ar: AttributeReference => {
        currentInputNode = inputWhyNotQuestion.rootNode.children.find(node => node.name == ar.name).getOrElse(SchemaNode(ar.name, Constraint(""), inputWhyNotQuestion.rootNode))
        currentInputNode.parent.addChild(currentInputNode)
      }
    }
    currentInputNode = currentInputNode.children.find(node => node.name == field.name.get).getOrElse(SchemaNode(field.name.get, Constraint(""), currentInputNode))
    currentInputNode.parent.addChild(currentInputNode)
    true
  }

  def backtraceGetStructField(field: GetStructField): Boolean = {
    val currentInputNode = this.currentInputNode
    val res = backtraceGetStructFieldInternal(field)
    if (!directChildOfAlias){
      currentOutputNode = currentOutputNode.getChild(currentInputNode.name).getOrElse(return false)
    }
    directChildOfAlias = false
    this.currentInputNode.constraint = currentOutputNode.constraint.deepCopy()
    this.currentInputNode = currentInputNode
    currentOutputNode = currentOutputNode.parent
    res
  }

  def backtraceGetStructField2(field: GetStructField): Boolean = {
    val name = field.name.getOrElse(return false)
    var valid = true

    /*
      Note: the field expression should have to be the leaf node in the tree
        and, thus, we investigate from the deepest child in the expression to be added to the tree
     */
    for (child <- field.children) {
      valid |= backtraceExpression(child)
    }

    /*
      Note:
        1) The "currentInputNode" is always the root node of the tree
        2) Need to find the correct path for the corresponding node to the field expression (in case the parent node has multiple children)
        3) Add the node as a leaf node
        4) Then, return to the root node
     */
    val childName = getChildName(field.child)
    currentInputNode = currentInputNode.getChild(childName).getOrElse(return false)
    currentInputNode = currentInputNode.getLeafNode()
    currentInputNode = currentInputNode.getChild(name).getOrElse(SchemaNode(name, currentInputNode.constraint.deepCopy(), currentInputNode))
    currentInputNode.parent.addChild(currentInputNode)
    currentInputNode = currentInputNode.getRootNode()
    valid
  }

  def getChildName(expression: Expression): String = {
    var childName = ""

    expression match {
      case al: Alias => childName = al.name
      case a: Attribute => childName = a.name
      case gs: GetStructField => childName = gs.name.get
    }

    childName
  }

//  def backtraceStructType(a: Attribute, st: StructType): Boolean = {
//    currentInputNode = currentInputNode.getChild(a.name).getOrElse(return false)
//
//    for (child <- st.fields) {
//      currentOutputNode = currentOutputNode.getChild(child.name).getOrElse(SchemaNode("", parent = currentOutputNode))
//      val newNode = SchemaNode(child.name, null, currentInputNode)
//
//      if (!currentOutputNode.name.equals("")) {
//        newNode.constraint = currentOutputNode.constraint.deepCopy()
//        currentInputNode.addChild(newNode)
//      }
//
//      currentOutputNode = currentOutputNode.parent
//    }
//
//    currentInputNode = currentInputNode.parent
//    true
//  }

  def backtraceLiteral(l: Literal): Boolean = {
    throw new MatchError("Literals are not supported, yet.")
  }

  def getInputTree(): SchemaSubsetTree = {
    backtraceExpressions()
    inputWhyNotQuestion
  }

//  def setInitialInputTree(initialInputTree: SchemaSubsetTree): Unit = {
//    inputWhyNotQuestion = initialInputTree
//    currentInputNode = inputWhyNotQuestion.rootNode
//  }




}
