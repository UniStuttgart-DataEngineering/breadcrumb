package de.uni_stuttgart.ipvs.provenance.schema_alternatives

import scala.collection.mutable


object PrimarySchemaNode {
  def apply(schemaNode: SchemaNode, parent: PrimarySchemaNode): PrimarySchemaNode = {
    new PrimarySchemaNode(schemaNode.name, schemaNode.constraint, parent)
  }
}

class PrimarySchemaNode(_name: String, _constraint: Constraint, _parent: SchemaNode = null) extends SchemaNode(_name, _constraint, _parent)  {
  val alternatives = scala.collection.mutable.ListBuffer.empty[SchemaNode]
  //override var children = scala.collection.mutable.Set.empty[PrimarySchemaNode]

  override def addChild(child: SchemaNode): Unit = {
    children += child
  }

  def getChildren: mutable.Set[PrimarySchemaNode] = {
    children.map{child => child.asInstanceOf[PrimarySchemaNode]}
  }

  def getParent(): PrimarySchemaNode = {
    if (parent == null) {
      return null
    }
    parent.asInstanceOf[PrimarySchemaNode]
  }

  def addAlternative(alternativeNode: SchemaNode) = {
    alternatives += alternativeNode
  }
}
