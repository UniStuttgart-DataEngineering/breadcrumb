package de.uni_stuttgart.ipvs.provenance.schema_alternatives

import org.apache.spark.sql.catalyst.expressions.{Alias, Attribute, AttributeReference, CreateNamedStruct, Expression, GetStructField, Literal}
import org.apache.spark.sql.types.{StructField, StructType}
import org.apache.spark.unsafe.types.UTF8String

object SchemaSubsetTreeModifications {
  def apply(outputWhyNotQuestion: SchemaSubsetTree, inputAttributes: Seq[Attribute], outputAttributes: Seq[Attribute], modificationExpressions: Seq[Expression]) = {
    new SchemaSubsetTreeModifications(outputWhyNotQuestion, inputAttributes, outputAttributes, modificationExpressions)
  }
}

class SchemaSubsetTreeModifications(outputWhyNotQuestion: SchemaSubsetTree, inputAttributes: Seq[Attribute],
                                    outputAttributes: Seq[Attribute], modificationExpressions: Seq[Expression]) {

  var inputWhyNotQuestion = SchemaSubsetTree()

  var currentOutputNode = outputWhyNotQuestion.rootNode
  var currentInputNode = inputWhyNotQuestion.rootNode

  var directChildOfAlias = false
  var generateAccess = false


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
    }

  }

  def backtraceGenerator(): SchemaSubsetTree = {
//    currentOutputNode = currentOutputNode.getChild(outputAttributes.head.name).getOrElse(return)
    val flattenedAttrName = outputAttributes.head.name
    currentOutputNode = currentOutputNode.getChild(flattenedAttrName).getOrElse(SchemaNode(flattenedAttrName, currentOutputNode.constraint, currentOutputNode))
    directChildOfAlias = true
    generateAccess = true
    val flattenedExpr = modificationExpressions.head
    backtraceExpression(flattenedExpr)
    generateAccess = false

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

    inputWhyNotQuestion
  }

  def backtraceAlias(alias: Alias): Boolean = {
    currentOutputNode = currentOutputNode.getChild(alias.name).getOrElse(return false)
    directChildOfAlias = true
    backtraceExpression(alias.child)
  }

  def backtraceAttribute(attribute: Attribute): Boolean = {
    val name = attribute.name

    if (!directChildOfAlias) {
      currentOutputNode = currentOutputNode.getChild(name).getOrElse(return false)
//      currentOutputNode = currentOutputNode.getChild(name).getOrElse(SchemaNode(name, currentOutputNode.constraint, currentOutputNode))
    }
    directChildOfAlias = false

    //TODO: if an attribute is referenced multiple times, constraints need special handling
    currentInputNode = currentInputNode.getChild(name).getOrElse(SchemaNode(name, currentOutputNode.constraint, currentInputNode))
    currentInputNode.parent.addChild(currentInputNode)

    if (generateAccess) {
      handleGenerateAccess()
    }

    copyChildrenOfOutputAttributeToInputAttribute()

    if (generateAccess) {
      currentInputNode = currentInputNode.parent
    }

    currentInputNode = currentInputNode.parent

    /*
    val newNode = SchemaNode(attribute.name, currentOutputNode.constraint, currentInputNode)
    currentInputNode.addChild(newNode)

    if (attribute.dataType.typeName.equals("struct") && currentInputNode.name.equals("root")) {
        backtraceStructType(attribute, attribute.dataType.asInstanceOf[StructType])
    }*/

    currentOutputNode = currentOutputNode.parent
    true
  }

  def copyChildrenOfOutputAttributeToInputAttribute(): Unit = {
    for (child <- currentOutputNode.children) {
      val newNode = child.deepCopy(currentInputNode)
      currentInputNode.addChild(newNode)
    }
  }

  def handleGenerateAccess(): Unit = {
    currentInputNode.constraint = Constraint("") // TODO: potentially faulty, if attribute already exists in the input schema subset
    val name = "element"
//    currentInputNode = currentInputNode.getChild(name).getOrElse(SchemaNode(name, currentOutputNode.constraint, currentInputNode))
//    currentInputNode.parent.addChild(currentOutputNode)
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

  def backtraceGetStructField(field: GetStructField): Boolean = {
    val name = field.name.getOrElse(return false)
    currentInputNode = currentInputNode.getChild(name).getOrElse(SchemaNode(name, parent = currentInputNode))
    currentInputNode.parent.addChild(currentInputNode)
    var valid = true
    for (child <- field.children) {
      valid |= backtraceExpression(child)
    }
    currentInputNode = currentInputNode.parent
    valid
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

  def setInitialInputTree(initialInputTree: SchemaSubsetTree): Unit = {
    inputWhyNotQuestion = initialInputTree
    currentInputNode = inputWhyNotQuestion.rootNode
  }




}
