package de.uni_stuttgart.ipvs.provenance.why_not_question

import org.apache.spark.sql.Row

class SchemaMatchTree() {
  var treeNodes: Array[UDFTreeNode] = null
  var root: UDFTreeNode = null

  def initialize(serializedNodeList: Seq[Row]): SchemaMatchTree ={
    treeNodes = new Array[UDFTreeNode](serializedNodeList.size)
    for (row <- serializedNodeList){
      val node = new UDFTreeNode().parseRow(row)
      treeNodes(node.id) = node
      node.addNodeToParent(treeNodes)
    }
    root = treeNodes(0)
    this
  }

}