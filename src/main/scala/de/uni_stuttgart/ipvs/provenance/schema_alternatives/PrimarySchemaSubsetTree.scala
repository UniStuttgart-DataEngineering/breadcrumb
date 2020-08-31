package de.uni_stuttgart.ipvs.provenance.schema_alternatives


object PrimarySchemaSubsetTree {

  def merge(leftTree: PrimarySchemaSubsetTree, rightTree: PrimarySchemaSubsetTree): PrimarySchemaSubsetTree ={
    val outputTree = PrimarySchemaSubsetTree(true)//new PrimarySchemaSubsetTree()
    val totalNumberOfAlternatives = leftTree.getAllAlternatives().size * rightTree.getAllAlternatives().size
    for (_ <- 1 until totalNumberOfAlternatives){
      val outputAlternative = SchemaSubsetTree(true)
      outputTree.addAlternative(outputAlternative)
      outputTree.getRootNode.addAlternative(outputAlternative.rootNode)
    }
    for (child <- leftTree.getRootNode.getChildren) {
      child.createDuplicates(rightTree.getAllAlternatives().size, outputTree.getRootNode, false)
    }
    for (child <- rightTree.getRootNode.getChildren) {
      child.createDuplicates(rightTree.getAllAlternatives().size, outputTree.getRootNode, true)
    }
    outputTree
  }

  def apply(schemaSubsetTree: SchemaSubsetTree): PrimarySchemaSubsetTree = {
    val primaryTree = new PrimarySchemaSubsetTree()
    primaryTree.rootNode = toPrimarySchemaNode(schemaSubsetTree.rootNode, null)
    primaryTree
  }

  def apply(withRootNode: Boolean): PrimarySchemaSubsetTree = {
    val primaryTree = new PrimarySchemaSubsetTree()
    primaryTree.rootNode = PrimarySchemaNode("root", Constraint(""), null)
    primaryTree
  }

  def apply(id: Int): PrimarySchemaSubsetTree = {
    val schemaSubsetTree = SchemaSubsetTree(true, id)
    apply(schemaSubsetTree)
  }

  def toPrimarySchemaNode(node: SchemaNode, parent: PrimarySchemaNode): PrimarySchemaNode = {
    val newNode = PrimarySchemaNode(node, parent)
    for (child <- node.children){
      newNode.addChild(toPrimarySchemaNode(child, newNode))
    }
    newNode
  }
}

class PrimarySchemaSubsetTree extends SchemaSubsetTree {
  var alternatives = scala.collection.mutable.ListBuffer.empty[SchemaSubsetTree]

  def getAllAlternatives(): Seq[SchemaSubsetTree] = {
     this :: alternatives.toList
  }

  def addAlternative(alternative: SchemaSubsetTree): Unit = {
    alternatives += alternative
  }

  def addRootOnlyAlternative(alternative: SchemaSubsetTree): Unit = {
    addAlternative(alternative)
    getRootNode.addAlternative(alternative.rootNode)
  }

  def getRootNode: PrimarySchemaNode = {
    rootNode match {
      case node: PrimarySchemaNode => node
      case _ => null
    }
  }
  //override var rootNode: PrimarySchemaNode = null
}
