package de.uni_stuttgart.ipvs.provenance.schema_alternatives

import de.uni_stuttgart.ipvs.provenance.why_not_question.{MatchNode, Schema, TwigNode}



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

  var inPathWithCondition = false

  def hasValueConstraint(): Boolean = {
    constraint.constraintString.nonEmpty
  }

  def setParent(_parent: SchemaNode) = {
    parent = _parent
  }

  def addChild(child: SchemaNode): Unit = {
    children += child
  }

  def removeChild(child: SchemaNode): Unit = {
    children -= child
  }

  def copyNode(node: SchemaNode): Unit = {
    name = node.name
    parent = node.parent
    constraint = node.constraint.deepCopy()
    for (child <- node.children){
      children.add(child.deepCopy(parent))
    }
  }

  def getLeafNode(): SchemaNode = {
    var leafNode = this

    while (!leafNode.children.isEmpty) {
      leafNode = leafNode.children.head
    }

    leafNode
  }

  def getRootNode(): SchemaNode = {
    var root = parent

    while (!root.name.equals("root")) {
      root = root.parent
    }

    root
  }

  def getChild(name: String): Option[SchemaNode] = {
    children.find(child => child.name == name)
  }

  def getChildByPos(pos: Int): SchemaNode = {
    var nthChild: Int = 0
    var nthChildNode: SchemaNode = null

    for (child <- children) {
      if (nthChild == pos) {
        nthChildNode = child
      }
      nthChild += 1
    }

    nthChildNode
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

  def deepCopyWithoutChildren(copiedParent: SchemaNode) = {
    val copiedName = name + ""
    val copiedConstrained = constraint.deepCopy()
    val copiedNode = SchemaNode(copiedName, copiedConstrained, copiedParent)
    copiedNode
  }

  def serialize(id: Short, parentId: Short):(Short, Short, Byte, Int, Int, String, String) = {
    (id, parentId, constraint.operatorId, constraint.min, constraint.max, name, constraint.attributeValue)
  }
}
