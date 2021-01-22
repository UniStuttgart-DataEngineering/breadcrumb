package de.uni_stuttgart.ipvs.provenance.evaluation.tpch

import de.uni_stuttgart.ipvs.provenance.schema_alternatives.{PrimarySchemaSubsetTree, SchemaNode}

object OrdersAlternatives extends TPCHAlternatives {

  override def createAllAlternatives(primarySchemaSubsetTree: PrimarySchemaSubsetTree): PrimarySchemaSubsetTree = {
    val tree = primarySchemaSubsetTree
    val alternatives = createAlternatives(tree, 1)
    replaceAlternative(alternatives.alternatives(0).rootNode)
    tree
  }


  def replaceAlternative(node: SchemaNode): Unit = {
    replaceOrderPriorityTMP(node)
    replaceShipPriorityTMP(node)
    replaceShipPriorityWithOrderPriority(node)
    replaceOrderPriorityWithShipPriority(node)
  }

  def replaceOrderPriorityTMP(node: SchemaNode): Unit ={
    replaceWithTMP(node, "o_orderpriority")
  }

  def replaceShipPriorityTMP(node: SchemaNode): Unit ={
    replaceWithTMP(node, "o_shippriority")
  }

  def replaceShipPriorityWithOrderPriority(node: SchemaNode): Unit ={
    if (node.name == "o_shippriority_tmp" && node.children.isEmpty) {
      node.name = "o_orderpriority"
      node.modified = true
      return
    }
    for (child <- node.children){
      replaceShipPriorityWithOrderPriority(child)
    }
  }

  def replaceOrderPriorityWithShipPriority(node: SchemaNode): Unit ={
    if (node.name == "o_orderpriority_tmp" && node.children.isEmpty) {
      node.name = "o_shippriority"
      node.modified = true
      return
    }
    for (child <- node.children){
      replaceOrderPriorityWithShipPriority(child)
    }
  }










}
