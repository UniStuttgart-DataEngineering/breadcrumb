package de.uni_stuttgart.ipvs.provenance.why_not_question

import org.apache.spark.sql.Row
import org.apache.spark.sql.api.java.UDF2

import scala.collection.mutable

class DataFetcherUDF extends UDF2[Row, Seq[Row], Boolean] {

  override def call(row: Row, other: Seq[Row]): Boolean = {
    val tree = buildTree(other)
    validate(row, tree.root)
    //tree.root.children.forall(child => validate(row, tree.root))
  }

  def validate(schemaNode: Any, treeNode: UDFTreeNode): Boolean = {
    schemaNode match {
      case x : String => validate(x, treeNode)
      case x : Int => validate(x, treeNode)
      case x : Boolean => validate(x, treeNode)
      case x : Row => validate(x, treeNode)
      case x : mutable.WrappedArray[_] => validate(x, treeNode)
      case x : Long => validate(x, treeNode)
      case null => false
      case x => throw new ClassCastException("DataFetcher does not support elements of type " + x.getClass)
    }
  }

  def validate(value: String, treeNode: UDFTreeNode): Boolean = {
    if (!treeNode.isLeaf()) return false //needed?
    if (value == null) return false
    treeNode.compareWithValue(value)
  }

  def validate(value: Int, treeNode: UDFTreeNode): Boolean = {
    if (!treeNode.isLeaf()) return false //needed?
    treeNode.compareWithValue(value)
  }

  def validate(value: Long, treeNode: UDFTreeNode): Boolean = {
    if (!treeNode.isLeaf()) return false //needed?
    treeNode.compareWithValue(value)
  }

  def validate(value: Boolean, treeNode: UDFTreeNode): Boolean = {
    if (!treeNode.isLeaf()) return false //needed?
    treeNode.compareWithValue(value)
  }

  def validate(value: Row, treeNode: UDFTreeNode): Boolean = {
    if (value == null) return false
    var valid = true
    for (child <- treeNode.children){
      try {
        val idx = value.fieldIndex(child.attributeName)
        valid &= validate(value.get(idx), child)   //potential for optimizations
      }
      catch {
        case i: IllegalArgumentException =>
          for (desc <- child.children) {
            validate(value, desc)
          }
      }
    }
    valid
  }

  def validate(value: mutable.WrappedArray[_], treeNode: UDFTreeNode): Boolean = {
    if (value == null) return false
    var valid = true
    val validItemCount = value.count(element => {
      if (treeNode.children.isEmpty) return true
      validate(element, treeNode.children.head)
    })
    // check min max values [if none is set but a attribute value, we say its once at least]
    // min <= max already checked on .validate, therefore we only check each
    if (valid) {
      valid &= treeNode.min <= validItemCount
      if (treeNode.max != -1) valid &= validItemCount <= treeNode.max
    }
    valid
  }

  def buildTree(treeNodes: Seq[Row]) : SchemaMatchTree = {
    new SchemaMatchTree().initialize(treeNodes)
  }

}
