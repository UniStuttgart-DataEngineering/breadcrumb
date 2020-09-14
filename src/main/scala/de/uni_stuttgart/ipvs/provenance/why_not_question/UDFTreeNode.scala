package de.uni_stuttgart.ipvs.provenance.why_not_question

import org.apache.spark.sql.Row

import scala.collection.mutable
import scala.util.Try

class UDFTreeNode() {

  var id: Short = 0
  var comparisonOp: Byte = 0
  var parentId: Short = 0
  var min: Int = 1
  var max: Int = -1 // ignore until set
  var attributeName: String = null
  var attributeValue: String = null
  var children = mutable.Set.empty[UDFTreeNode]

  def addChild(child: UDFTreeNode) = {
    if (child != this) {
      children += child
    }
  }

  //def id = _id

  def getParent(treeNodes:Array[UDFTreeNode]): UDFTreeNode = {
    treeNodes(parentId)
  }

  def addNodeToParent(treeNodes:Array[UDFTreeNode]) : Unit = {
    getParent(treeNodes).addChild(this)
  }

  def isLeaf() : Boolean = {
    children.isEmpty
  }

  def compareWithValue(value: String): Boolean = {
    comparisonOp match {
      case 0 => true
      case 1 => attributeValue == value
      case 2 => value.contains(attributeValue)
      case 5 => !value.contains(attributeValue)
      case 6 => value.length > attributeValue.toInt
      case 7 => value.length < attributeValue.toInt
      case _ => false
    }
  }


  def compareWithValue(value: Int): Boolean = {
    if (comparisonOp == 0 && attributeValue == "") return true
    val attributeVal: Int = Try(attributeValue.toInt).getOrElse(return false)
    comparisonOp match {
      case 0 => true
      case 1 => attributeVal == value
      case 3 => attributeVal > value
      case 4 => attributeVal < value
      case _ => false
    }
  }

  def compareWithValue(value: Double): Boolean = {
    if (comparisonOp == 0 && attributeValue == "") return true
    val attributeVal: Double = Try(attributeValue.toDouble).getOrElse(return false)
    comparisonOp match {
      case 0 => true
      case 1 => attributeVal == value
      case 3 => attributeVal > value
      case 4 => attributeVal < value
      case _ => false
    }
  }

  def compareWithValue(value: Long): Boolean = {
    if (comparisonOp == 0 && attributeValue == "") return true
    val attributeVal: Long = Try(attributeValue.toLong).getOrElse(return false)
    comparisonOp match {
      case 0 => true
      case 1 => attributeVal == value
      case 3 => attributeVal > value
      case 4 => attributeVal < value
      case _ => false
    }
  }



  def compareWithValue(value: Boolean): Boolean = {
    if (comparisonOp == 0 && attributeValue == "") return true
    val attributeVal: Boolean = Try(attributeValue.toBoolean).getOrElse(return false)
    comparisonOp match {
      case 0 => true
      case 1 => attributeVal == value
      case _ => false
    }

  }

  def parseRow(row: Row) : UDFTreeNode = {
    id = row.getAs[Short](0) // id
    parentId = row.getAs[Short](1)
    comparisonOp = row.getAs[Byte](2) // comparison_operator
    min = row.getAs[Int](3) // min
    max = row.getAs[Int](4) // max
    attributeName = row.getAs[String](5) // attribute_name
    attributeValue = row.getAs[String](6) // attribute_value
    this
  }

  /**
   * needed for visualizing. don't change the strings, only adjust the numbers if needed.
   * strings are the same in the javascript files..
   * @return
   */
  def getComparisonOpAsReadable : String = {
    comparisonOp match {
      case 1 => "equals"
      case 2 => "contains"
      case 3 => "lessThan"
      case 4 => "greaterThan"
      case 5 => "containsNot"
      case _ => ""
    }
  }

}