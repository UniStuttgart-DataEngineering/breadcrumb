package de.uni_stuttgart.ipvs.provenance.schema_alternatives

import de.uni_stuttgart.ipvs.provenance.why_not_question.{MatchNode, Schema, TwigNode}

case class Constraint(constraintString: String, min: Int = 1, max: Int = -1) {

  val operatorId = getOperatorId(constraintString)
  val attributeValue = getAttributeValue(constraintString, operatorId)


  private def getPrefix(condition: String):String = {
    condition.substring(0, Math.min(8,condition.length))
  }

  private def getOperatorId(condition: String): Byte = {
    getPrefix(condition) match {
      case "contains" => 2
      case "ltltltlt" => 3
      case "gtgtgtgt" => 4
      case "nconncon" => 5
      case "lengthgt" => 6
      case "lengthlt" => 7
      case x => if (x.length() > 0) 1 else 0
    }
  }

  private def getAttributeValue(condition: String, operatorId: Byte): String = {
    if (operatorId < 2) condition else condition.substring(Math.min(8, condition.length-1))
  }

  def deepCopy(): Constraint = {
    Constraint(constraintString + "", min, max)
  }

}

object SchemaNode {

  private def toConstraint(matchNode: MatchNode) = {
    var min = 1
    var max = -1
    var condition = ""
    matchNode.getTwigNode() match {
      case Some(tNode) => {
        min = tNode.min;
        max = tNode.max;
        condition = tNode.condition
      }
      case None => {}
    }
    Constraint(condition, min, max)
  }

  private def fromMatchNode(matchNode: MatchNode, schema: Schema, parent: SchemaNode): SchemaNode = {
    val name = matchNode.getName(schema)
    val constraint = toConstraint(matchNode)
    new SchemaNode(name, constraint, parent)
  }

  def apply(matchNode: MatchNode, schema: Schema, parent: SchemaNode) = fromMatchNode(matchNode, schema, parent)

  def apply(name: String, constraint: Constraint = Constraint(""), parent: SchemaNode = null) = new SchemaNode(name, constraint, parent)

}

class SchemaNode(_name: String, _constraint: Constraint, _parent: SchemaNode = null) {

  var name = _name
  var parent : SchemaNode = _parent
  var children = scala.collection.mutable.Set.empty[SchemaNode]
  var constraint = _constraint

  def setParent(_parent: SchemaNode) = {
    parent = _parent
  }

  def addChild(child: SchemaNode) = {
    children += child
  }

  def getChild(name: String): Option[SchemaNode] = {
    children.find(child => child.name == name)
  }

  def rename(newName: String): Unit = {
    name = newName
  }

  def deepCopy(copiedParent: SchemaNode): SchemaNode = {
    val copiedName = name + ""
    val copiedConstrained = constraint.deepCopy()
    val copiedNode = SchemaNode(copiedName, copiedConstrained, copiedParent)
    for (child <- children){
      copiedNode.addChild(child.deepCopy(copiedNode))
    }
    copiedNode
  }

  def serialize(id: Short, parentId: Short):(Short, Short, Byte, Int, Int, String, String) = {
    (id, parentId, constraint.operatorId, constraint.min, constraint.max, name, constraint.attributeValue)
  }
}
