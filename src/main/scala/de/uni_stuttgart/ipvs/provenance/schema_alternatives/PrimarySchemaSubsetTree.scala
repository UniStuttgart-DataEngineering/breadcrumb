package de.uni_stuttgart.ipvs.provenance.schema_alternatives


object PrimarySchemaSubsetTree {
  def apply(schemaSubsetTree: SchemaSubsetTree): PrimarySchemaSubsetTree = {
    val primaryTree = new PrimarySchemaSubsetTree()
    primaryTree.rootNode = toPrimarySchemaNode(schemaSubsetTree.rootNode, null)
    primaryTree
  }

  def toPrimarySchemaNode(node: SchemaNode, parent: PrimarySchemaNode): PrimarySchemaNode = {
    val newNode = PrimarySchemaNode(node, parent)
    for (child <- node.children){
      newNode.addChild(toPrimarySchemaNode(child, parent))
    }
    newNode
  }
}

class PrimarySchemaSubsetTree extends SchemaSubsetTree {
  var alternatives = scala.collection.mutable.ListBuffer.empty[SchemaSubsetTree]

  def getAllAlternatives(): Seq[SchemaSubsetTree] = {
     this :: alternatives.toList
  }

  def getRootNode: PrimarySchemaNode = {
    rootNode match {
      case node: PrimarySchemaNode => node
      case _ => null
    }
  }
  //override var rootNode: PrimarySchemaNode = null
}
