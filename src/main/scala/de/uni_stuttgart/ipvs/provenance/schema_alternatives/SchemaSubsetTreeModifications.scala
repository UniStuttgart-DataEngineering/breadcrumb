package de.uni_stuttgart.ipvs.provenance.schema_alternatives

import org.apache.spark.sql.catalyst.expressions.{Alias, Attribute, AttributeReference, CreateNamedStruct, Expression, GetStructField, Literal}
import org.apache.spark.sql.types.{StructField, StructType}
import org.apache.spark.unsafe.types.UTF8String

object SchemaSubsetTreeModifications {
  def apply(outputWhyNotQuestion: SchemaSubsetTree, inputAttributes: Seq[Attribute], outputAttributes: Seq[Attribute], modificationExpressions: Seq[Expression]) = {
    new SchemaSubsetTreeModifications(outputWhyNotQuestion, inputAttributes, outputAttributes, modificationExpressions)
  }
}

class SchemaSubsetTreeModifications(outputWhyNotQuestion: SchemaSubsetTree, inputAttributes: Seq[Attribute], outputAttributes: Seq[Attribute], modificationExpressions: Seq[Expression]) {

  val inputWhyNotQuestion = SchemaSubsetTree()

  var currentOutputNode = outputWhyNotQuestion.rootNode
  var currentInputNode = inputWhyNotQuestion.rootNode

  var directChildOfAlias = false


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
        backtraceAttribute(a)
      }
      case gs: GetStructField => {
        backtraceGetStructField(gs)
      }
      case af: AttributeReference => {
        backtraceAttributeReference(af)
      }
      case l: Literal => {
        backtraceLiteral(l)
      }
    }

  }


  def backtraceAlias(alias: Alias): Boolean = {
    currentOutputNode = currentOutputNode.getChild(alias.name).getOrElse(return false)
    directChildOfAlias = true
    backtraceExpression(alias.child)
  }

  def backtraceAttribute(attribute: Attribute): Boolean = {
    if (!directChildOfAlias) {
      currentOutputNode = currentOutputNode.getChild(attribute.name).getOrElse(return false)

    }
    directChildOfAlias = false
    val newNode = SchemaNode(attribute.name, currentOutputNode.constraint, currentInputNode)
    currentInputNode.addChild(newNode)
    currentOutputNode = currentOutputNode.parent
    true
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

  def backtraceAttributeReference(attributeReference: AttributeReference): Boolean = {
    val newNode = SchemaNode(attributeReference.name, currentOutputNode.constraint, currentInputNode)
    currentInputNode.addChild(newNode)
    currentInputNode = newNode
    true
  }

  def backtraceLiteral(l: Literal): Boolean = {
    throw new MatchError("Literals are not supported, yet.")
  }

  def getInputTree(): SchemaSubsetTree = {
    backtraceExpressions()
    inputWhyNotQuestion
  }




}
