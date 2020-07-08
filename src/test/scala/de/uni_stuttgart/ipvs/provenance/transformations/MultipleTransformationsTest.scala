package de.uni_stuttgart.ipvs.provenance.transformations

import de.uni_stuttgart.ipvs.provenance.SharedSparkTestDataFrames
import de.uni_stuttgart.ipvs.provenance.nested_why_not.{Constants, WhyNotProvenance}
import de.uni_stuttgart.ipvs.provenance.schema_alternatives.SchemaSubsetTree
import de.uni_stuttgart.ipvs.provenance.why_not_question.{Schema, SchemaBackTrace, Twig}
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.catalyst.plans.logical.{Filter, Join, LeafNode, LogicalPlan, Project}
import org.apache.spark.sql.functions.{explode, struct}
import org.scalatest.FunSuite

class MultipleTransformationsTest extends FunSuite with SharedSparkTestDataFrames {

  import spark.implicits._

  def nestedWhyNotTuple(): Twig = {
    var twig = new Twig()
    val root = twig.createNode("root", 1, 1, "")
    val nested_list = twig.createNode("nested_list", 1, 1, "")
    val element1 = twig.createNode("element", 1, 1, "1_list_val_1_1")
    twig = twig.createEdge(root, nested_list, false)
    twig = twig.createEdge(nested_list, element1, false)
    twig.validate().get
  }

  test("[Rewrite] Explode contains survived field") {
    val df = getDataFrame(pathToNestedData00)
    var otherDf = df.withColumn("flattened", explode($"nested_list"))
    otherDf = otherDf.filter($"flat_key" === "1_flat_val_x")
    otherDf = otherDf.select($"nested_list")
    val res = WhyNotProvenance.rewrite(otherDf, nestedWhyNotTuple)
    res.explain(true)
    res.show()
  }

//  def getInputAndOutputWhyNotTuple(outputDataFrame: DataFrame, outputWhyNotTuple: Twig): (SchemaSubsetTree, SchemaSubsetTree, SchemaSubsetTree) = {
//    val schemaMatch = getSchemaMatch(outputDataFrame, outputWhyNotTuple)
//    val schemaSubset = SchemaSubsetTree(schemaMatch, new Schema(outputDataFrame))
//
//    val plan = outputDataFrame.queryExecution.analyzed
//    val child = plan.children.head
//
//    var rewrite1: SchemaSubsetTree = null
//    var rewrite2: SchemaSubsetTree = null
//
//    plan match {
//      case f: Filter => rewrite1 = FilterRewrite(f, -1).undoSchemaModifications(schemaSubset)
//      case p: Project => rewrite1 = ProjectRewrite(p, -1).undoSchemaModifications(schemaSubset)
//      case _ =>
//    }
//
//    child match {
//      case f: Filter => rewrite2 = FilterRewrite(f, 0).undoSchemaModifications(rewrite1)
//      case p: Project => rewrite2 = ProjectRewrite(p, 0).undoSchemaModifications(rewrite1)
//      case _ =>
//    }
//
//    (rewrite1, rewrite2, schemaSubset)
//
////    val rewrite = SchemaBackTrace(plan, schemaSubset)
////    val rewrite2 = SchemaBackTrace(plan.children.head, rewrite.unrestructure().head)
//
////    (rewrite.unrestructure().head, rewrite2.unrestructure().head, schemaSubset)
//  }


//  def getInputAndOutputWhyNotTuple2(outputDataFrame: DataFrame, outputWhyNotTuple: Twig): (Seq[SchemaSubsetTree], Seq[SchemaSubsetTree], SchemaSubsetTree) = {
//    val schemaMatch = getSchemaMatch(outputDataFrame, outputWhyNotTuple)
//    val schemaSubset = SchemaSubsetTree(schemaMatch, new Schema(outputDataFrame))
//
//    val plan = outputDataFrame.queryExecution.analyzed
//    val rewrite = SchemaBackTrace(plan, schemaSubset)
//    val rewrite2 = SchemaBackTrace(plan.children.head, rewrite.unrestructure().head)
//
//    (rewrite.unrestructure(), rewrite2.unrestructure(), schemaSubset)
//  }


  def whyNotTupleProjectionNewName(newName: String): Twig = {
    var twig = new Twig()
    val root = twig.createNode("root", 1, 1, "")
    val flat_key = twig.createNode(newName, 1, 1, "")
    twig = twig.createEdge(root, flat_key, false)
    twig.validate().get
  }


  test("[Unrestructure] Filter over Project (Attribute is renamed)") {
    val newName = "renamed"
    val df = getDataFrame(pathToNestedData00)
    var res = df.select($"flat_key".alias(newName))
    res = res.filter($"renamed" === "1_flat_val_x")

    val plan = res.queryExecution.analyzed
    val schemaSubset = getSchemaSubsetTree(res, whyNotTupleProjectionNewName(newName))
    val rewrittenSchemaSubset1 = getInputAndOutputWhyNotTupleFlex(plan, schemaSubset, "")
    val rewrittenSchemaSubset2 = getInputAndOutputWhyNotTupleFlex(plan.children.head, rewrittenSchemaSubset1, "")

//    val (rewrittenSchemaSubset1, rewrittenSchemaSubset2, schemaSubset) = getInputAndOutputWhyNotTuple(res, whyNotTupleProjectionNewName(newName))

    assert(schemaSubset.rootNode.name == rewrittenSchemaSubset1.rootNode.name)
    assert(schemaSubset.rootNode.name == rewrittenSchemaSubset2.rootNode.name)
    assert(rewrittenSchemaSubset2.rootNode.children.head.name == "flat_key")
    assert(rewrittenSchemaSubset1.rootNode.children.head.name != rewrittenSchemaSubset2.rootNode.children.head.name)
  }


  test("[Unrestructure] Project over Filter (Attribute is renamed)") {
    val newName = "renamed"
    val df = getDataFrame(pathToNestedData00)
    var res = df.filter($"flat_key" === "1_flat_val_x")
    res = res.select($"flat_key".alias(newName))

    val plan = res.queryExecution.analyzed
    val schemaSubset = getSchemaSubsetTree(res, whyNotTupleProjectionNewName(newName))
    val rewrittenSchemaSubset1 = getInputAndOutputWhyNotTupleFlex(plan, schemaSubset, "")
    val rewrittenSchemaSubset2 = getInputAndOutputWhyNotTupleFlex(plan.children.head, rewrittenSchemaSubset1, "")

//    val (rewrittenSchemaSubset1, rewrittenSchemaSubset2, schemaSubset) = getInputAndOutputWhyNotTuple(res, whyNotTupleProjectionNewName(newName))

    assert(schemaSubset.rootNode.name == rewrittenSchemaSubset1.rootNode.name)
    assert(schemaSubset.rootNode.name == rewrittenSchemaSubset2.rootNode.name)
    assert(rewrittenSchemaSubset1.rootNode.children.head.name == "flat_key")
    assert(rewrittenSchemaSubset1.rootNode.children.head.name == rewrittenSchemaSubset2.rootNode.children.head.name)
  }


  test("[Unrestructure] Filter over nested tuple") {
    //    val newName = "renamed"
    val df = getDataFrame(pathToNestedData00)
    var res = df.select($"obj")
    res = res.filter($"obj.obj_sub.key" === 1)

    val plan = res.queryExecution.analyzed
    val schemaSubset = getSchemaSubsetTree(res, whyNotTupleProjectionNewName("obj"))
    val rewrittenSchemaSubset1 = getInputAndOutputWhyNotTupleFlex(plan, schemaSubset, "")
    val rewrittenSchemaSubset2 = getInputAndOutputWhyNotTupleFlex(plan.children.head, rewrittenSchemaSubset1, "")

//    val (rewrittenSchemaSubset1, rewrittenSchemaSubset2, schemaSubset) = getInputAndOutputWhyNotTuple(res, whyNotTupleProjectionNewName("obj"))

    assert(schemaSubset.rootNode.name == rewrittenSchemaSubset1.rootNode.name)
    assert(schemaSubset.rootNode.name == rewrittenSchemaSubset2.rootNode.name)

    assert(rewrittenSchemaSubset2.rootNode.children.head.name == "obj")
//    assert(rewrittenSchemaSubset2.rootNode.children.head.children.head.name == "obj_sub")
//    assert(rewrittenSchemaSubset2.rootNode.children.head.children.head.children.head.name == "key")
  }


  test("[Unrestructure] Filter over renamed nested tuple") {
    val newName = "renamed"
    val df = getDataFrame(pathToNestedData00)
    var res = df.select($"obj".alias(newName))
    res = res.filter($"renamed.obj_sub.key" === 1)

    val plan = res.queryExecution.analyzed
    val schemaSubset = getSchemaSubsetTree(res, whyNotTupleProjectionNewName(newName))
    val rewrittenSchemaSubset1 = getInputAndOutputWhyNotTupleFlex(plan, schemaSubset, "")
    val rewrittenSchemaSubset2 = getInputAndOutputWhyNotTupleFlex(plan.children.head, rewrittenSchemaSubset1, "")

//    val (rewrittenSchemaSubset1, rewrittenSchemaSubset2, schemaSubset) = getInputAndOutputWhyNotTuple(res, whyNotTupleProjectionNewName(newName))

    assert(schemaSubset.rootNode.name == rewrittenSchemaSubset1.rootNode.name)
    assert(schemaSubset.rootNode.name == rewrittenSchemaSubset2.rootNode.name)

    assert(rewrittenSchemaSubset2.rootNode.children.head.name == "obj")
//    assert(rewrittenSchemaSubset2.rootNode.children.head.children.head.name == "obj_sub")
//    assert(rewrittenSchemaSubset2.rootNode.children.head.children.head.children.head.name == "key")
  }


  test("[Unrestructure] Filter over nested tuple with multiple columns") {
    //    val newName = "renamed"
    val df = getDataFrame(pathToDemoData)
    var res = df.select($"user")
    res = res.filter($"user.name" === "Lisa Paul")

    val plan = res.queryExecution.analyzed
    val schemaSubset = getSchemaSubsetTree(res, whyNotTupleProjectionNewName("user"))
    val rewrittenSchemaSubset1 = getInputAndOutputWhyNotTupleFlex(plan, schemaSubset, "")
    val rewrittenSchemaSubset2 = getInputAndOutputWhyNotTupleFlex(plan.children.head, rewrittenSchemaSubset1, "")

//    val (rewrittenSchemaSubset1, rewrittenSchemaSubset2, schemaSubset) = getInputAndOutputWhyNotTuple(res, whyNotTupleProjectionNewName("user"))

    assert(schemaSubset.rootNode.name == rewrittenSchemaSubset1.rootNode.name)
    assert(schemaSubset.rootNode.name == rewrittenSchemaSubset2.rootNode.name)

//    val id = rewrittenSchemaSubset2.rootNode.children.head.children.find(node => node.name == "id_str").getOrElse(fail("id_str not where it is supposed to be"))
//    val userName = rewrittenSchemaSubset2.rootNode.children.head.children.find(node => node.name == "name").getOrElse(fail("name not where it is supposed to be"))

    assert(rewrittenSchemaSubset2.rootNode.children.head.name == "user")
//    assert(id.name == "id_str")
//    assert(userName.name == "name")
  }


  def whyNotTupleStruct(newName: String, newSubName: String): Twig = {
    var twig = new Twig()
    val root = twig.createNode("root", 1, 1, "")
    val nested_obj1 = twig.createNode(newName, 1, 1, "")
    val nested_obj2 = twig.createNode(newSubName, 1, 1, "")
    //    val nested_key = twig.createNode("key")
    twig = twig.createEdge(root, nested_obj1, false)
    twig = twig.createEdge(nested_obj1, nested_obj2, false)
    //    twig = twig.createEdge(nested_obj2, nested_key, false)
    twig.validate().get
  }


  test("[Unrestructure] Filter over nested tuple 2") {
    //    val newName = "renamed"
    val df = getDataFrame(pathToNestedData00)
    var res = df.select($"obj")
    res = res.filter($"obj.obj_sub.key" === 1)

    val plan = res.queryExecution.analyzed
    val schemaSubset = getSchemaSubsetTree(res, whyNotTupleStruct("obj", "obj_sub"))
    val rewrittenSchemaSubset1 = getInputAndOutputWhyNotTupleFlex(plan, schemaSubset, "")
    val rewrittenSchemaSubset2 = getInputAndOutputWhyNotTupleFlex(plan.children.head, rewrittenSchemaSubset1, "")

//    val (rewrittenSchemaSubset1, rewrittenSchemaSubset2, schemaSubset) = getInputAndOutputWhyNotTuple(res, whyNotTupleStruct("obj", "obj_sub"))

    assert(schemaSubset.rootNode.name == rewrittenSchemaSubset1.rootNode.name)
    assert(schemaSubset.rootNode.name == rewrittenSchemaSubset2.rootNode.name)

    assert(rewrittenSchemaSubset2.rootNode.children.head.name == "obj")
    assert(rewrittenSchemaSubset2.rootNode.children.head.children.head.name == "obj_sub")
//    assert(rewrittenSchemaSubset2.rootNode.children.head.children.head.children.head.name == "key")
  }



  test("[Unrestructure] Filter over nested tuple with multiple columns, but reference one attribute") {
    //    val newName = "renamed"
    val df = getDataFrame(pathToDemoData)
    var res = df.select($"user")
    res = res.filter($"user.name" === "Lisa Paul")

    val plan = res.queryExecution.analyzed
    val schemaSubset = getSchemaSubsetTree(res, whyNotTupleStruct("user", "id_str"))
    val rewrittenSchemaSubset1 = getInputAndOutputWhyNotTupleFlex(plan, schemaSubset, "")
    val rewrittenSchemaSubset2 = getInputAndOutputWhyNotTupleFlex(plan.children.head, rewrittenSchemaSubset1, "")

//    val (rewrittenSchemaSubset1, rewrittenSchemaSubset2, schemaSubset) = getInputAndOutputWhyNotTuple(res, whyNotTupleStruct("user", "id_str"))

    assert(schemaSubset.rootNode.name == rewrittenSchemaSubset1.rootNode.name)
    assert(schemaSubset.rootNode.name == rewrittenSchemaSubset2.rootNode.name)

    assert(rewrittenSchemaSubset2.rootNode.children.head.name == "user")
    assert(rewrittenSchemaSubset2.rootNode.children.head.children.head.name == "id_str")

    val size = rewrittenSchemaSubset2.rootNode.children.size
    assert(size == 1)
  }


  def whyNotTupleJoinAndProject(): Twig = {
    var twig = new Twig()
    val root = twig.createNode("root", 1, 1, "")
    val value = twig.createNode("value", 1, 1, "10")
    val value2 = twig.createNode("otherValue", 1, 1, "100")
    twig = twig.createEdge(root, value, false)
    twig = twig.createEdge(root, value2, false)
    twig.validate().get
  }


//  test("[Unrestructure] Join without renaming attributes over projection") {
//    val dfLeft = getDataFrame(pathToAggregationDoc0)
//    val dfRight = getDataFrame(pathToJoinDoc0)
//    var res = dfLeft.join(dfRight, Seq("key"))
//    res = res.select($"value", $"otherValue")
//
//    val (rewrittenSchemaSubset1, rewrittenSchemaSubset2, schemaSubset) = getInputAndOutputWhyNotTuple2(res, whyNotTupleJoinAndProject())
//
//    val projectRewrittenSchemaSubset  = rewrittenSchemaSubset1.head
//    val leftRewrittenSchemaSubset = rewrittenSchemaSubset2.head
//    val rightRewrittenSchemaSubset = rewrittenSchemaSubset2.last
//
//    // Test after Project
//    assert(schemaSubset.rootNode.name == projectRewrittenSchemaSubset.rootNode.name)
//
//    var value = projectRewrittenSchemaSubset.rootNode.children.find(node => node.name == "value").getOrElse(fail("value not where it is supposed to be"))
//    var value2 = projectRewrittenSchemaSubset.rootNode.children.find(node => node.name == "otherValue").getOrElse(fail("otherValue not where it is supposed to be"))
//
//    assert(value.name == "value")
//    assert(value2.name == "otherValue")
//
//    // Test after Join
//    assert(schemaSubset.rootNode.name == leftRewrittenSchemaSubset.rootNode.name)
//    assert(schemaSubset.rootNode.name == rightRewrittenSchemaSubset.rootNode.name)
//
//    value = leftRewrittenSchemaSubset.rootNode.children.find(node => node.name == "value").getOrElse(fail("value not where it is supposed to be"))
//    assert(value.name == "value")
//
//    value2 = rightRewrittenSchemaSubset.rootNode.children.find(node => node.name == "otherValue").getOrElse(fail("otherValue not where it is supposed to be"))
//    assert(value2.name == "otherValue")
//  }


  test("[Unrestructure] Join without renaming attributes over projection 2") {
    val dfLeft = getDataFrame(pathToAggregationDoc0)
    val dfRight = getDataFrame(pathToJoinDoc0).withColumnRenamed("key", "key2")
    var res = dfLeft.join(dfRight, $"key" === $"key2")
    res = res.select($"value", $"otherValue")

    // Evaluate over Project
    val plan = res.queryExecution.analyzed
    val schemaSubset = getSchemaSubsetTree(res, whyNotTupleJoinAndProject())
    val rewrittenSchemaSubsetProject = getInputAndOutputWhyNotTupleFlex(plan, schemaSubset, "")

    // Evaluate over Join
    val lChild = plan.children.head
    val rChild = plan.children.last

    val rewrittenSchemaSubsetLeft = getInputAndOutputWhyNotTupleFlex(lChild, rewrittenSchemaSubsetProject, "L")
    val rewrittenSchemaSubsetRight = getInputAndOutputWhyNotTupleFlex(rChild, rewrittenSchemaSubsetProject, "R")

    // Test after Project
    assert(schemaSubset.rootNode.name == rewrittenSchemaSubsetProject.rootNode.name)

    var value = rewrittenSchemaSubsetProject.rootNode.children.find(node => node.name == "value").getOrElse(fail("value not where it is supposed to be"))
    var value2 = rewrittenSchemaSubsetProject.rootNode.children.find(node => node.name == "otherValue").getOrElse(fail("otherValue not where it is supposed to be"))

    assert(value.name == "value")
    assert(value2.name == "otherValue")

    // Test after Join
    assert(schemaSubset.rootNode.name == rewrittenSchemaSubsetLeft.rootNode.name)
    assert(schemaSubset.rootNode.name == rewrittenSchemaSubsetRight.rootNode.name)

    value = rewrittenSchemaSubsetLeft.rootNode.children.find(node => node.name == "value").getOrElse(fail("value not where it is supposed to be"))
    assert(value.name == "value")

    value2 = rewrittenSchemaSubsetRight.rootNode.children.find(node => node.name == "otherValue").getOrElse(fail("otherValue not where it is supposed to be"))
    assert(value2.name == "otherValue")
  }


  def whyNotTupleJoinAndProject2(): Twig = {
    var twig = new Twig()
    val root = twig.createNode("root", 1, 1, "")
    val value = twig.createNode("value", 1, 1, "10")
    val value2 = twig.createNode("value2", 1, 1, "100")
    twig = twig.createEdge(root, value, false)
    twig = twig.createEdge(root, value2, false)
    twig.validate().get
  }


//  test("[Unrestructure] Join with renaming attributes over projection") {
//    val dfLeft = getDataFrame(pathToAggregationDoc0)
//    val dfRight = getDataFrame(pathToJoinDoc0)
//    var res = dfLeft.join(dfRight, Seq("key"))
//    res = res.select($"value", $"otherValue".alias("value2"))
//
//    val (rewrittenSchemaSubset1, rewrittenSchemaSubset2, schemaSubset) = getInputAndOutputWhyNotTuple2(res, whyNotTupleJoinAndProject2())
//
//    val projectRewrittenSchemaSubset  = rewrittenSchemaSubset1.head
//    val leftRewrittenSchemaSubset = rewrittenSchemaSubset2.head
//    val rightRewrittenSchemaSubset = rewrittenSchemaSubset2.last
//
//    // Test after Project
//    assert(schemaSubset.rootNode.name == projectRewrittenSchemaSubset.rootNode.name)
//
//    var value = projectRewrittenSchemaSubset.rootNode.children.find(node => node.name == "value").getOrElse(fail("value not where it is supposed to be"))
//    var value2 = projectRewrittenSchemaSubset.rootNode.children.find(node => node.name == "otherValue").getOrElse(fail("otherValue not where it is supposed to be"))
//
//    assert(value.name == "value")
//    assert(value2.name == "otherValue")
//
//    // Test after Join
//    assert(schemaSubset.rootNode.name == leftRewrittenSchemaSubset.rootNode.name)
//    assert(schemaSubset.rootNode.name == rightRewrittenSchemaSubset.rootNode.name)
//
//    value = leftRewrittenSchemaSubset.rootNode.children.find(node => node.name == "value").getOrElse(fail("value not where it is supposed to be"))
//    assert(value.name == "value")
//
//    value2 = rightRewrittenSchemaSubset.rootNode.children.find(node => node.name == "otherValue").getOrElse(fail("otherValue not where it is supposed to be"))
//    assert(value2.name == "otherValue")
//  }


  test("[Unrestructure] Join with renaming attributes over projection 2") {
    val dfLeft = getDataFrame(pathToAggregationDoc0)
    val dfRight = getDataFrame(pathToJoinDoc0).withColumnRenamed("key", "key2")
    var res = dfLeft.join(dfRight, $"key" === $"key2")
    res = res.select($"value", $"otherValue".alias("value2"))

    // Evaluate over Project
    val plan = res.queryExecution.analyzed
    val schemaSubset = getSchemaSubsetTree(res, whyNotTupleJoinAndProject2())
    val rewrittenSchemaSubsetProject = getInputAndOutputWhyNotTupleFlex(plan, schemaSubset, "")

    // Evaluate over Join
    val lChild = plan.children.head
    val rChild = plan.children.last

    val rewrittenSchemaSubsetLeft = getInputAndOutputWhyNotTupleFlex(lChild, rewrittenSchemaSubsetProject, "L")
    val rewrittenSchemaSubsetRight = getInputAndOutputWhyNotTupleFlex(rChild, rewrittenSchemaSubsetProject, "R")
//
//    val (rewrittenSchemaSubset1, rewrittenSchemaSubset2, schemaSubset) = getInputAndOutputWhyNotTuple2(res, whyNotTupleJoinAndProject2())
//
//    val projectRewrittenSchemaSubset  = rewrittenSchemaSubset1.head
//    val leftRewrittenSchemaSubset = rewrittenSchemaSubset2.head
//    val rightRewrittenSchemaSubset = rewrittenSchemaSubset2.last

    // Test after Project
    assert(schemaSubset.rootNode.name == rewrittenSchemaSubsetProject.rootNode.name)

    var value = rewrittenSchemaSubsetProject.rootNode.children.find(node => node.name == "value").getOrElse(fail("value not where it is supposed to be"))
    var value2 = rewrittenSchemaSubsetProject.rootNode.children.find(node => node.name == "otherValue").getOrElse(fail("otherValue not where it is supposed to be"))

    assert(value.name == "value")
    assert(value2.name == "otherValue")

    // Test after Join
    assert(schemaSubset.rootNode.name == rewrittenSchemaSubsetLeft.rootNode.name)
    assert(schemaSubset.rootNode.name == rewrittenSchemaSubsetRight.rootNode.name)

    value = rewrittenSchemaSubsetLeft.rootNode.children.find(node => node.name == "value").getOrElse(fail("value not where it is supposed to be"))
    assert(value.name == "value")

    value2 = rewrittenSchemaSubsetRight.rootNode.children.find(node => node.name == "otherValue").getOrElse(fail("otherValue not where it is supposed to be"))
    assert(value2.name == "otherValue")
  }


//  test("[Unrestructure] Join over Filter") {
//    val dfLeft = getDataFrame(pathToAggregationDoc0)
//    val dfRight = getDataFrame(pathToJoinDoc0)
//    var res = dfLeft.join(dfRight, Seq("key"))
//    res = res.filter($"value" === 100)
//
//    val (rewrittenSchemaSubset1, rewrittenSchemaSubset2, schemaSubset) = getInputAndOutputWhyNotTuple2(res, whyNotTupleJoinAndProject())
//
//    val filterRewrittenSchemaSubset  = rewrittenSchemaSubset1.head
//    val leftRewrittenSchemaSubset = rewrittenSchemaSubset2.head
//    val rightRewrittenSchemaSubset = rewrittenSchemaSubset2.last
//
//    // Test after Project
//    assert(schemaSubset.rootNode.name == filterRewrittenSchemaSubset.rootNode.name)
//
//    var value = filterRewrittenSchemaSubset.rootNode.children.find(node => node.name == "value").getOrElse(fail("value not where it is supposed to be"))
//    var value2 = filterRewrittenSchemaSubset.rootNode.children.find(node => node.name == "otherValue").getOrElse(fail("otherValue not where it is supposed to be"))
//
//    assert(value.name == "value")
//    assert(value2.name == "otherValue")
//
//    // Test after Join
//    assert(schemaSubset.rootNode.name == leftRewrittenSchemaSubset.rootNode.name)
//    assert(schemaSubset.rootNode.name == rightRewrittenSchemaSubset.rootNode.name)
//
//    value = leftRewrittenSchemaSubset.rootNode.children.find(node => node.name == "value").getOrElse(fail("value not where it is supposed to be"))
//    assert(value.name == "value")
//
//    value2 = rightRewrittenSchemaSubset.rootNode.children.find(node => node.name == "otherValue").getOrElse(fail("otherValue not where it is supposed to be"))
//    assert(value2.name == "otherValue")
//  }


  test("[Unrestructure] Join over Filter 2") {
    val dfLeft = getDataFrame(pathToAggregationDoc0)
    val dfRight = getDataFrame(pathToJoinDoc0).withColumnRenamed("key", "key2")
    var res = dfLeft.join(dfRight, $"key" === $"key2")
    res = res.filter($"value" === 100)

    // Evaluate over Filter
    val plan = res.queryExecution.analyzed
    val schemaSubset = getSchemaSubsetTree(res, whyNotTupleJoinAndProject())
    val filterRewrittenSchemaSubset = getInputAndOutputWhyNotTupleFlex(plan, schemaSubset, "")

    // Evaluate over Join
    val lChild = plan.children.head
    val rChild = plan.children.last

    val leftRewrittenSchemaSubset = getInputAndOutputWhyNotTupleFlex(lChild, filterRewrittenSchemaSubset, "L")
    val rightRewrittenSchemaSubset = getInputAndOutputWhyNotTupleFlex(rChild, filterRewrittenSchemaSubset, "R")

//    val (rewrittenSchemaSubset1, rewrittenSchemaSubset2, schemaSubset) = getInputAndOutputWhyNotTuple2(res, whyNotTupleJoinAndProject())
//
//    val filterRewrittenSchemaSubset  = rewrittenSchemaSubset1.head
//    val leftRewrittenSchemaSubset = rewrittenSchemaSubset2.head
//    val rightRewrittenSchemaSubset = rewrittenSchemaSubset2.last

    // Test after Project
    assert(schemaSubset.rootNode.name == filterRewrittenSchemaSubset.rootNode.name)

    var value = filterRewrittenSchemaSubset.rootNode.children.find(node => node.name == "value").getOrElse(fail("value not where it is supposed to be"))
    var value2 = filterRewrittenSchemaSubset.rootNode.children.find(node => node.name == "otherValue").getOrElse(fail("otherValue not where it is supposed to be"))

    assert(value.name == "value")
    assert(value2.name == "otherValue")

    // Test after Join
    assert(schemaSubset.rootNode.name == leftRewrittenSchemaSubset.rootNode.name)
    assert(schemaSubset.rootNode.name == rightRewrittenSchemaSubset.rootNode.name)

    value = leftRewrittenSchemaSubset.rootNode.children.find(node => node.name == "value").getOrElse(fail("value not where it is supposed to be"))
    assert(value.name == "value")

    value2 = rightRewrittenSchemaSubset.rootNode.children.find(node => node.name == "otherValue").getOrElse(fail("otherValue not where it is supposed to be"))
    assert(value2.name == "otherValue")
  }


}
