package de.uni_stuttgart.ipvs.provenance.transformations

import com.github.mrpowers.spark.fast.tests.DataFrameComparer
import de.uni_stuttgart.ipvs.provenance.SharedSparkTestDataFrames
import de.uni_stuttgart.ipvs.provenance.nested_why_not.{Constants, WhyNotProvenance}
import de.uni_stuttgart.ipvs.provenance.schema_alternatives.{SchemaSubsetTree, SchemaSubsetTreeModifications}
import de.uni_stuttgart.ipvs.provenance.why_not_question.{Schema, SchemaBackTrace, SchemaMatch, SchemaMatcher, Twig}
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.catalyst.plans.logical.Project
import org.apache.spark.sql.functions._
import org.scalatest.FunSuite

class ProjectTest extends FunSuite with SharedSparkTestDataFrames with DataFrameComparer {

  import spark.implicits._

  def getInputAndOutputWhyNotTuple(outputDataFrame: DataFrame, outputWhyNotTuple: Twig): (SchemaSubsetTree, SchemaSubsetTree) = {
//    val schemaMatch = getSchemaMatch(outputDataFrame, whyNotQuestionFlatKey())
    val schemaMatch = getSchemaMatch(outputDataFrame, outputWhyNotTuple)
    val schemaSubset = SchemaSubsetTree(schemaMatch, new Schema(outputDataFrame))
    val rewrite = ProjectRewrite(outputDataFrame.queryExecution.analyzed.asInstanceOf[Project], -1)
    //val rewrite = SchemaSubsetTreeModifications(schemaSubset, )//SchemaBackTrace(outputDataFrame.queryExecution.analyzed, schemaSubset)
    (rewrite.undoSchemaModifications(schemaSubset), schemaSubset) // (inputWhyNotTuple, outputWhyNotTuple)
  }

//  def getWhyNotTupleOverInput(inputDataFrame: DataFrame, outputDataFrame: DataFrame, outputWhyNotTuple: Twig): (SchemaSubsetTree, SchemaSubsetTree) = {
      //TODO: labels of input schema should be retrieved in output schema
//    val inSchema = new Schema(inputDataFrame)
//    val outSchema = new Schema(outputDataFrame)
//
//    // SchemaSubsetTree over output
//    val schemaMatch = getSchemaMatch(outputDataFrame, outputWhyNotTuple)
//    val schemaSubset = SchemaSubsetTree(schemaMatch, outSchema)
//
//    // SchemaSubsetTree over input
//    val schemaMatchOverInput = getSchemaMatchNew(inSchema,outSchema,outputWhyNotTuple)
//    val schemaSubsetOverInput = SchemaSubsetTree(schemaMatchOverInput, inSchema)
//
//    (schemaSubsetOverInput, schemaSubset)
//  }

//  test("[Rewrite + Unrestructure] Correct unstructuring over rewrite") {
//    val df = singleInputColumnDataFrame()
//    val res1 = WhyNotProvenance.rewrite(df, whyNotTupleWithCond())
//    val dfAfterProj = df.select($"MyIntCol")
//    val res2 = WhyNotProvenance.rewrite(dfAfterProj, whyNotTupleWithCond())
//
////    println(res1.queryExecution.analyzed)
////    println(res2.queryExecution.analyzed)
////
////    res1.show()
////    res2.show()
//
//    val compatibleOverDf = df.select($"__COMPATIBLE_0002")
//    val compatibleOverDfAfterProj = df.select($"__COMPATIBLE_0005")
//
//    compatibleOverDf.show()
//    compatibleOverDfAfterProj.show()
//
//    assert(res1.schema.size == res2.schema.size)
//    assert(res1.schema.size == 2)
//    assert(compatibleOverDf == compatibleOverDfAfterProj)
//  }


  test("[Rewrite] Projection keeps all provenance columns after rewrite") {
    val df = singleInputColumnDataFrame()
    val res1 = WhyNotProvenance.rewrite(df, myIntColWhyNotQuestion())
    val otherDf = df.select($"MyIntCol")
    val res2 = WhyNotProvenance.rewrite(otherDf, myIntColWhyNotQuestion())
    assert(res1.schema.size == res2.schema.size)
    assert(res1.schema.size == 2)
  }

  test("[Rewrite] Projection keeps values in provenance columns") {
    val df = singleInputColumnDataFrame()
    val res1 = WhyNotProvenance.rewrite(df, myIntColWhyNotQuestion())
    val otherDf = df.select($"MyIntCol")
    val res2 = WhyNotProvenance.rewrite(otherDf, myIntColWhyNotQuestion())
    assertSmallDataFrameEquality(res1, res2, ignoreColumnNames = true)
  }

  def whyNotQuestionFlatKey(): Twig = {
    var twig = new Twig()
    val root = twig.createNode("root", 1, 1, "")
    val flat_key = twig.createNode("flat_key", 1, 1, "")
    twig = twig.createEdge(root, flat_key, false)
    twig.validate().get
  }

  def whyNotQuestionFlatKeyWithCondition(): Twig = {
    var twig = new Twig()
    val root = twig.createNode("root", 1, 1, "")
    val flat_key = twig.createNode("flat_key", 1, 1, "1_2_flat_val")
    twig = twig.createEdge(root, flat_key, false)
    twig.validate().get
  }

  def whyNotQuestionFlatKeyWithCondition2(): Twig = {
    var twig = new Twig()
    val root = twig.createNode("root", 1, 1, "")
    val flat_key = twig.createNode("myName", 1, 1, "1_2_flat_val")
    twig = twig.createEdge(root, flat_key, false)
    twig.validate().get
  }

  def whyNotQuestionFull(): Twig = {
    var twig = new Twig()
    val root = twig.createNode("root", 1, 1, "")
    val nested_obj1 = twig.createNode("nested_obj", 1, 1, "")
    val nested_obj2 = twig.createNode("nested_obj", 1, 1, "")
    val nested_key = twig.createNode("nested_key", 1, 1, "")
    twig = twig.createEdge(root, nested_obj1, false)
    twig = twig.createEdge(nested_obj1, nested_obj2, false)
    twig = twig.createEdge(nested_obj2, nested_key, false)
    twig.validate().get
  }




  test("[Unrestructure] Attribute keeps name and structure"){
    val df = getDataFrame()
    val res = df.select($"flat_key")
    val (rewrittenSchemaSubset, schemaSubset) = getInputAndOutputWhyNotTuple(res, whyNotQuestionFlatKey())
//    val (rewrittenSchemaSubset, schemaSubset) = getWhyNotTupleOverInput(df, res, whyNotQuestionFlatKey())
    (rewrittenSchemaSubset, schemaSubset)

    assert(schemaSubset.rootNode.name == rewrittenSchemaSubset.rootNode.name)
    assert(schemaSubset.rootNode.children.head.name == rewrittenSchemaSubset.rootNode.children.head.name)
  }

  def whyNotTupleProjectionNewName(newName: String): Twig = {
    var twig = new Twig()
    val root = twig.createNode("root", 1, 1, "")
    val flat_key = twig.createNode(newName, 1, 1, "")
    twig = twig.createEdge(root, flat_key, false)
    twig.validate().get
  }

  test("[Unrestructure] Attribute is renamed"){
    val newName = "renamed"
    val df = getDataFrame()
    val res = df.select($"flat_key".alias(newName))
//    whyNotTupleProjectionNewName(newName)

    val (rewrittenSchemaSubset, schemaSubset) = getInputAndOutputWhyNotTuple(res, whyNotTupleProjectionNewName(newName))
//    val (rewrittenSchemaSubset, schemaSubset) = getWhyNotTupleOverInput(df, res, whyNotTupleProjectionNewName(newName))

    assert(schemaSubset.rootNode.name == rewrittenSchemaSubset.rootNode.name)
    assert(rewrittenSchemaSubset.rootNode.children.head.name == "flat_key")
  }

  test("[Unrestructure] Nested attribute is not renamed"){
    val df = getDataFrame()
    val res = df.select($"nested_obj.nested_obj")

    val (rewrittenSchemaSubset, schemaSubset) = getInputAndOutputWhyNotTuple(res, whyNotTupleProjectionNewName("nested_obj"))
//    val (rewrittenSchemaSubset, schemaSubset) = getWhyNotTupleOverInput(df, res, whyNotTupleProjectionNewName("nested_obj"))

    assert(schemaSubset.rootNode.name == rewrittenSchemaSubset.rootNode.name)
    assert(rewrittenSchemaSubset.rootNode.children.head.name == "nested_obj")
  }

  test("[Unrestructure] Nested attribute is renamed"){
    val newName = "renamed"
    val df = getDataFrame()
    val res = df.select($"nested_obj.nested_obj".alias(newName))

    val (rewrittenSchemaSubset, schemaSubset) = getInputAndOutputWhyNotTuple(res, whyNotTupleProjectionNewName(newName))
//    val (rewrittenSchemaSubset, schemaSubset) = getWhyNotTupleOverInput(df, res, whyNotTupleProjectionNewName(newName))

    assert(schemaSubset.rootNode.name == rewrittenSchemaSubset.rootNode.name)
    assert(rewrittenSchemaSubset.rootNode.children.head.name == "nested_obj")
  }

  test("[Unrestructure] Nested attribute is renamed 2"){
    val newName = "renamed"
    val df = getDataFrame()
    val res = df.select($"nested_obj.nested_obj".alias(newName))

    val (rewrittenSchemaSubset, schemaSubset) = getInputAndOutputWhyNotTuple(res, whyNotTupleProjectionNewName(newName))
    val nested_obj = rewrittenSchemaSubset.rootNode.children.head
    val nested_obj2 = nested_obj.children.head

    assert(schemaSubset.rootNode.name == rewrittenSchemaSubset.rootNode.name)
    assert(nested_obj.name == "nested_obj")
    assert(nested_obj2.name == "nested_obj")
  }

  def whyNotQuestionWithNesting(newName: String): Twig = {
    var twig = new Twig()
    val root = twig.createNode("root", 1, 1, "")
    val newNameNode = twig.createNode(newName, 1, 1, "")
    val flat_key = twig.createNode("flat_key", 1, 1, "")
    twig = twig.createEdge(root, newNameNode, false)
    twig = twig.createEdge(newNameNode, flat_key, false)
    twig.validate().get
  }

  test("[Unrestructure] Flat attribute is nested"){
    val newName = "tupleName"
    val df = getDataFrame()
    val res = df.select(struct($"flat_key").alias(newName))

    val (rewrittenSchemaSubset, schemaSubset) = getInputAndOutputWhyNotTuple(res, whyNotQuestionWithNesting(newName))


    val flat_key = rewrittenSchemaSubset.rootNode.children.find(node => node.name == "flat_key").getOrElse(fail("flat_key not where it is supposed to be"))
    assert(schemaSubset.rootNode.name == rewrittenSchemaSubset.rootNode.name)
    assert(flat_key.name == "flat_key")

  }

  def whyNotQuestionWithNesting2(newName: String): Twig = {
    var twig = new Twig()
    val root = twig.createNode("root", 1, 1, "")
    val newNameNode = twig.createNode(newName, 1, 1, "")
    val flat_key = twig.createNode("flat_key", 1, 1, "")
    val nested_obj = twig.createNode("nested_obj", 1, 1, "")
    twig = twig.createEdge(root, newNameNode, false)
    twig = twig.createEdge(newNameNode, flat_key, false)
    twig = twig.createEdge(newNameNode, nested_obj, false)
    twig.validate().get
  }

  test("[Unrestructure] Create multiple attributes in a structure"){
    val newName1 = "tupleOne"
    val df = getDataFrame()
    val res = df.select(struct($"flat_key", $"nested_obj").alias(newName1))

    val (rewrittenSchemaSubset, schemaSubset) = getInputAndOutputWhyNotTuple(res, whyNotQuestionWithNesting2(newName1))


    val flat_key = rewrittenSchemaSubset.rootNode.children.find(node => node.name == "flat_key").getOrElse(fail("flat_key not where it is supposed to be"))
    val nested_obj = rewrittenSchemaSubset.rootNode.children.find(node => node.name == "nested_obj").getOrElse(fail("nested_obj not where it is supposed to be"))

    assert(flat_key.name == "flat_key")
    assert(nested_obj.name == "nested_obj")
  }

  test("[Unrestructure] Create multiple attributes in a structure, but reference just one attribute"){
    val newName1 = "tupleOne"
    val df = getDataFrame()
    val res = df.select(struct($"flat_key", $"nested_obj").alias(newName1))

    val (rewrittenSchemaSubset, schemaSubset) = getInputAndOutputWhyNotTuple(res, whyNotQuestionWithNesting(newName1))


    val flat_key = rewrittenSchemaSubset.rootNode.children.find(node => node.name == "flat_key").getOrElse(fail("flat_key not where it is supposed to be"))
    val size = rewrittenSchemaSubset.rootNode.children.size

    assert(flat_key.name == "flat_key")
    assert(size == 1)
  }

  def whyNotQuestionWithNesting3(newName1: String, newName2: String): Twig = {
    var twig = new Twig()
    val root = twig.createNode("root", 1, 1, "")
    val newNameNode = twig.createNode(newName1, 1, 1, "")
    val newNameNode2 = twig.createNode(newName2, 1, 1, "")
    val flat_key = twig.createNode("flat_key", 1, 1, "")
    val nested_obj = twig.createNode("nested_obj", 1, 1, "")
    twig = twig.createEdge(root, newNameNode, false)
    twig = twig.createEdge(newNameNode, flat_key, false)
    twig = twig.createEdge(newNameNode, newNameNode2, false)
    twig = twig.createEdge(newNameNode2, nested_obj, false)
    twig.validate().get
  }

  def whyNotQuestionWithNesting3prime(newName1: String, newName2: String, newName3: String): Twig = {
    var twig = new Twig()
    val root = twig.createNode("root", 1, 1, "")
    val newNameNode = twig.createNode(newName1, 1, 1, "")
    val newNameNode2 = twig.createNode(newName2, 1, 1, "")
    val flat_key = twig.createNode("flat_key", 1, 1, "")
    val nested_obj = twig.createNode(newName3, 1, 1, "")
    twig = twig.createEdge(root, newNameNode, false)
    twig = twig.createEdge(newNameNode, flat_key, false)
    twig = twig.createEdge(newNameNode, newNameNode2, false)
    twig = twig.createEdge(newNameNode2, nested_obj, false)
    twig.validate().get
  }

  test("[Unrestructure] Create multiple attributes in multiple structures"){
    val newName1 = "tupleOne"
    val newName2 = "tupleTwo"
    val df = getDataFrame()
    val res = df.select(struct($"flat_key", struct($"nested_obj").alias(newName2)).alias(newName1))

    val (rewrittenSchemaSubset, schemaSubset) = getInputAndOutputWhyNotTuple(res, whyNotQuestionWithNesting3(newName1, newName2))

    val flat_key = rewrittenSchemaSubset.rootNode.children.find(node => node.name == "flat_key").getOrElse(fail("flat_key not where it is supposed to be"))
    val nested_obj = rewrittenSchemaSubset.rootNode.children.find(node => node.name == "nested_obj").getOrElse(fail("nested_obj not where it is supposed to be"))

    assert(flat_key.name == "flat_key")
    assert(nested_obj.name == "nested_obj")
  }

  test("[Unrestructure] Create multiple attributes in multiple structures 2"){
    val newName1 = "tupleOne"
    val newName2 = "tupleTwo"
    val newName3 = "tupleThree"
    val df = getDataFrame()
    val res = df.select(struct($"flat_key", struct($"nested_obj".alias(newName3)).alias(newName2)).alias(newName1))

    val (rewrittenSchemaSubset, schemaSubset) = getInputAndOutputWhyNotTuple(res, whyNotQuestionWithNesting3prime(newName1, newName2, newName3))

    val flat_key = rewrittenSchemaSubset.rootNode.children.find(node => node.name == "flat_key").getOrElse(fail("flat_key not where it is supposed to be"))
    val nested_obj = rewrittenSchemaSubset.rootNode.children.find(node => node.name == "nested_obj").getOrElse(fail("nested_obj not where it is supposed to be"))

    assert(flat_key.name == "flat_key")
    assert(nested_obj.name == "nested_obj")
  }

  def whyNotQuestionNestedElement(): Twig = {
    var twig = new Twig()
    val root = twig.createNode("root", 1, 1, "")
    val nested_key = twig.createNode("nested_key", 1, 1, "")
    twig = twig.createEdge(root, nested_key, false)
    twig.validate().get
  }

  test("[Unrestructure] Access nested attribute"){
    val df = getDataFrame()
    val res = df.select($"nested_obj.nested_obj.nested_key")

    val (rewrittenSchemaSubset, _) = getInputAndOutputWhyNotTuple(res, whyNotQuestionNestedElement())


    val nested_obj1 = rewrittenSchemaSubset.rootNode.children.headOption.getOrElse(fail("nested_obj not where it is supposed to be"))
    assert(nested_obj1.name == "nested_obj")
    val nested_obj2 = nested_obj1.children.headOption.getOrElse(fail("nested_obj not where it is supposed to be"))
    assert(nested_obj2.name == "nested_obj")
    val nested_key = nested_obj2.children.headOption.getOrElse(fail("nested_key not where it is supposed to be"))
    assert(nested_key.name == "nested_key")
  }


  test("[Unrestructure] Create multiple attributes in multiple structures and multiple unnesting"){
    val newName1 = "tupleOne"
    val newName2 = "tupleTwo"
    val df = getDataFrame()
    val res = df.select(struct($"flat_key", struct($"nested_obj.nested_obj").alias(newName2)).alias(newName1), $"flat_key")

    val (rewrittenSchemaSubset, schemaSubset) = getInputAndOutputWhyNotTuple(res, whyNotQuestionWithNesting3(newName1, newName2))

    val flat_key = rewrittenSchemaSubset.rootNode.children.find(node => node.name == "flat_key").getOrElse(fail("flat_key not where it is supposed to be"))
    val nested_obj1 = rewrittenSchemaSubset.rootNode.children.find(node => node.name == "nested_obj").getOrElse(fail("nested_obj not where it is supposed to be"))
    val nested_obj2 = nested_obj1.children.find(node => node.name == "nested_obj").getOrElse(fail("nested_obj not where it is supposed to be"))


    assert(flat_key.name == "flat_key")
    assert(nested_obj1.name == "nested_obj")
    assert(nested_obj2.name == "nested_obj")
  }

  test("[Unrestructure] Create multiple attributes in multiple structures and multiple unnesting, but reference just one WN Question"){
    val newName1 = "tupleOne"
    val newName2 = "tupleTwo"
    val df = getDataFrame()
    val res = df.select(struct($"flat_key", struct($"nested_obj.nested_obj").alias(newName2)).alias(newName1), $"flat_key")

    val (rewrittenSchemaSubset, schemaSubset) = getInputAndOutputWhyNotTuple(res, whyNotQuestionFlatKey)



    val flat_key = rewrittenSchemaSubset.rootNode.children.find(node => node.name == "flat_key").getOrElse(fail("flat_key not where it is supposed to be"))
    assert(rewrittenSchemaSubset.rootNode.children.size == 1)

  }

  def whyNotQuestionWithNesting4(newName1: String, newName2: String): Twig = {
    var twig = new Twig()
    val root = twig.createNode("root", 1, 1, "")
    val newNameNode = twig.createNode(newName1, 1, 1, "")
    val newNameNode2 = twig.createNode(newName2, 1, 1, "")
    val nested_obj = twig.createNode("nested_obj", 1, 1, "")
    twig = twig.createEdge(root, newNameNode, false)
    twig = twig.createEdge(newNameNode, newNameNode2, false)
    twig = twig.createEdge(newNameNode2, nested_obj, false)
    twig.validate().get
  }

  test("[Unrestructure] Create multiple attributes in multiple structures but only one attribute 1"){
    val newName1 = "tupleOne"
    val newName2 = "tupleTwo"
    val df = getDataFrame()
    val res = df.select(struct($"flat_key", struct($"nested_obj.nested_obj").alias(newName2)).alias(newName1))

    val (rewrittenSchemaSubset, schemaSubset) = getInputAndOutputWhyNotTuple(res, whyNotQuestionWithNesting4(newName1, newName2))

    val nested_obj1 = rewrittenSchemaSubset.rootNode.children.find(node => node.name == "nested_obj").getOrElse(fail("nested_obj not where it is supposed to be"))
    val nested_obj2 = nested_obj1.children.find(node => node.name == "nested_obj").getOrElse(fail("nested_obj not where it is supposed to be"))
    val size1 = rewrittenSchemaSubset.rootNode.children.size
    val size2 = rewrittenSchemaSubset.rootNode.children.size

    assert(schemaSubset.rootNode.name == rewrittenSchemaSubset.rootNode.name)
    assert(nested_obj1.name == "nested_obj")
    assert(nested_obj2.name == "nested_obj")
    assert(size1 == 1)
    assert(size2 == 1)
  }

  test("[Unrestructure] Create multiple attributes in multiple structures but only one attribute 2"){
    val newName1 = "tupleOne"
    val newName2 = "tupleTwo"
    val df = getDataFrame()
    val res = df.select(struct($"flat_key", struct($"nested_obj.nested_obj").alias(newName2)).alias(newName1))

    val (rewrittenSchemaSubset, schemaSubset) = getInputAndOutputWhyNotTuple(res, whyNotQuestionWithNesting(newName1))

    val flat_key = rewrittenSchemaSubset.rootNode.children.find(node => node.name == "flat_key").getOrElse(fail("flat_key not where it is supposed to be"))
//    val nested_obj1 = rewrittenSchemaSubset.rootNode.children.find(node => node.name == "nested_obj").getOrElse(fail("nested_obj not where it is supposed to be"))
    val size = rewrittenSchemaSubset.rootNode.children.size

    assert(schemaSubset.rootNode.name == rewrittenSchemaSubset.rootNode.name)
    assert(flat_key.name == "flat_key")
    assert(size == 1)

  }

  test("[Unrestructure] Preserve Constraints"){
    val df = getDataFrame()
    val res = df.select($"flat_key")
    val (rewrittenSchemaSubset, schemaSubset) = getInputAndOutputWhyNotTuple(res, whyNotQuestionFlatKeyWithCondition())
    //    val (rewrittenSchemaSubset, schemaSubset) = getWhyNotTupleOverInput(df, res, whyNotQuestionFlatKey())
    (rewrittenSchemaSubset, schemaSubset)

    assert(schemaSubset.rootNode.children.head.constraint.attributeValue == "1_2_flat_val")
    assert(schemaSubset.rootNode.children.head.constraint.attributeValue == rewrittenSchemaSubset.rootNode.children.head.constraint.attributeValue)
  }

  test("[Unrestructure] Preserve Constraints after rename"){
    val df = getDataFrame()
    val res = df.select($"flat_key".alias("myName"))
    val (rewrittenSchemaSubset, schemaSubset) = getInputAndOutputWhyNotTuple(res, whyNotQuestionFlatKeyWithCondition2())
    //    val (rewrittenSchemaSubset, schemaSubset) = getWhyNotTupleOverInput(df, res, whyNotQuestionFlatKey())
    (rewrittenSchemaSubset, schemaSubset)

    assert(schemaSubset.rootNode.children.head.constraint.attributeValue == "1_2_flat_val")
    assert(schemaSubset.rootNode.children.head.constraint.attributeValue == rewrittenSchemaSubset.rootNode.children.head.constraint.attributeValue)
  }

}