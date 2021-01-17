package de.uni_stuttgart.ipvs.provenance.evaluation.tpch

import de.uni_stuttgart.ipvs.provenance.schema_alternatives.{PrimarySchemaSubsetTree, SchemaNode, SchemaSubsetTree}

abstract class TPCHAlternatives {

  def createAlternatives(primarySchemaSubsetTree: PrimarySchemaSubsetTree, numAlternatives: Int): PrimarySchemaSubsetTree = {
    for (altIdx <- 0 until numAlternatives) {
      val alternative = SchemaSubsetTree()
      primarySchemaSubsetTree.addAlternative(alternative)
      primarySchemaSubsetTree.getRootNode.addAlternative(alternative.rootNode)
    }
    for (child <- primarySchemaSubsetTree.getRootNode.getChildren) {
      child.createDuplicates(100, primarySchemaSubsetTree.getRootNode, false, false)
    }
    primarySchemaSubsetTree
  }

  def createAllAlternatives(primarySchemaSubsetTree: PrimarySchemaSubsetTree): PrimarySchemaSubsetTree

  private def tmpName(name: String) = {
    name + "_tmp"
  }

  def replaceWithTMP(node: SchemaNode, attributeName: String): Unit = {
    if (node.name == attributeName && node.children.isEmpty) {
      node.name = tmpName(attributeName)
      return
    }
    for (child <- node.children){
      replaceWithTMP(child, attributeName)
    }
  }

  def replaceTMPWithAlternative(node: SchemaNode, originalName: String, newName: String): Unit = {
    val tmpName = this.tmpName(originalName)
    if (node.name == tmpName && node.children.isEmpty) {
      node.name = newName
      if (originalName != newName){
        node.modified = true
      }
      return
    }
    for (child <- node.children){
      replaceTMPWithAlternative(child, originalName, newName)
    }
  }

}
