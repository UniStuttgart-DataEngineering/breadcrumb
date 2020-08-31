package de.uni_stuttgart.ipvs.provenance.schema_alternatives

import scala.collection.mutable


object PrimarySchemaNode {
  def apply(schemaNode: SchemaNode, parent: PrimarySchemaNode): PrimarySchemaNode = {
    new PrimarySchemaNode(schemaNode.name, schemaNode.constraint, parent)
  }

  def apply(name: String, constraint: Constraint, parent: PrimarySchemaNode): PrimarySchemaNode = {
    new PrimarySchemaNode(name, constraint, parent)
  }
}

class PrimarySchemaNode(_name: String, _constraint: Constraint, _parent: SchemaNode = null) extends SchemaNode(_name, _constraint, _parent)  {
  val alternatives = scala.collection.mutable.ListBuffer.empty[SchemaNode]
  //override var children = scala.collection.mutable.Set.empty[PrimarySchemaNode]

  override def addChild(child: SchemaNode): Unit = {
    children += child
  }

  def getAllAlternatives(): Seq[SchemaNode] = {
    this :: alternatives.toList
  }

  def deepCopyPrimary(copiedParent: PrimarySchemaNode): PrimarySchemaNode = {
    val copiedName = name + ""
    val copiedConstrained = constraint.deepCopy()
    val copiedNode = PrimarySchemaNode(copiedName, copiedConstrained, copiedParent)
    copiedParent.addChild(copiedNode)
    for ((alternative, parent) <- alternatives zip copiedParent.alternatives){
      val copy = alternative.deepCopy(parent)
      copiedNode.addAlternative(copy)
      parent.addChild(copy)
    }
    for (child <- getChildren){
      child.deepCopyPrimary(copiedNode)
    }
    copiedNode
  }

  def createDuplicates(alternatingFactor: Int, parent: PrimarySchemaNode, alternating: Boolean, copyPrimary: Boolean = true): Unit ={
    val copiedName = name + ""
    val copiedConstrained = constraint.deepCopy()
    val copiedNode = if(copyPrimary) PrimarySchemaNode(copiedName, copiedConstrained, parent) else this
    parent.addChild(copiedNode)
    val inputAlternatives = getAllAlternatives()
    for ((alternative, idx) <- parent.alternatives.zipWithIndex){
      val inputIdx: Int = if (alternating) (idx + 1) % alternatingFactor else (idx + 1) / alternatingFactor
      val copy = inputAlternatives(inputIdx).deepCopy(alternative)
      copiedNode.addAlternative(copy)
      alternative.addChild(copy)
    }
    for (child <- getChildren){
      child.createDuplicates(alternatingFactor, copiedNode,alternating)
    }
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

  def getPrimaryChild(name: String): Option[PrimarySchemaNode] = {
    val finding = children.find(child => child.name == name)
    finding match {

      case Some(child : PrimarySchemaNode) => {
        Some(child)
      }
      case _ => None
    }


  }
}
