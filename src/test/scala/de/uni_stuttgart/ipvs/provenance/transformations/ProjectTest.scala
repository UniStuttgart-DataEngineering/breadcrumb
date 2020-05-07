package de.uni_stuttgart.ipvs.provenance.transformations

import com.github.mrpowers.spark.fast.tests.DataFrameComparer
import de.uni_stuttgart.ipvs.provenance.SharedSparkTestDataFrames
import de.uni_stuttgart.ipvs.provenance.nested_why_not.{Constants, WhyNotProvenance}
import de.uni_stuttgart.ipvs.provenance.schema_alternatives.SchemaSubsetTree
import de.uni_stuttgart.ipvs.provenance.why_not_question.{Schema, SchemaMatch, SchemaMatcher, Twig}
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.catalyst.plans.logical.Project
import org.apache.spark.sql.functions._
import org.scalatest.FunSuite

class ProjectTest extends FunSuite with SharedSparkTestDataFrames with DataFrameComparer {

  import spark.implicits._




  def getInputAndOutputWhyNotTuple(outputDataFrame: DataFrame, outputWhyNotTuple: Twig): (SchemaSubsetTree, SchemaSubsetTree) = {
    val schemaMatch = getSchemaMatch(outputDataFrame, whyNotQuestionFlatKey())
    val schemaSubset = SchemaSubsetTree(schemaMatch, new Schema(outputDataFrame))
    val rewrite = ProjectRewrite(outputDataFrame.queryExecution.analyzed.asInstanceOf[Project],schemaSubset, 1)
    (rewrite.unrestructure(), schemaSubset) // (inputWhyNotTuple, outputWhyNotTuple)
  }

  test("Projection without provenance annotations does not add provenance annotations") {

    val df = singleInputColumnDataFrame()
    val otherDf = df.select($"MyIntCol")
    WhyNotProvenance.rewrite(otherDf, whyNotTuple())
    assertSmallDataFrameEquality(df, otherDf)
  }

  test("Projection with provenance annotations keeps provenance annotations") {

    val df = singleInputColumnDataFrame().withColumn(Constants.PROVENANCE_ID_STRUCT, monotonically_increasing_id())
    var otherDf = df.select($"MyIntCol")
    otherDf = WhyNotProvenance.rewrite(otherDf, whyNotTuple())
    otherDf.explain()
    otherDf.show()
    assertSmallDataFrameEquality(df, otherDf)
  }

  def whyNotQuestionFlatKey(): Twig = {
    var twig = new Twig()
    val root = twig.createNode("root", 1, 1, "")
    val flat_key = twig.createNode("flat_key", 1, 1, "")
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
    whyNotTupleProjectionNewName(newName)

    val (rewrittenSchemaSubset, schemaSubset) = getInputAndOutputWhyNotTuple(res, whyNotTupleProjectionNewName(newName))

    assert(schemaSubset.rootNode.name == rewrittenSchemaSubset.rootNode.name)
    assert(rewrittenSchemaSubset.rootNode.children.head.name == "flat_key")
  }

  test("[Unrestructure] Nested attribute is not renamed"){
    val df = getDataFrame()
    val res = df.select($"nested_obj.nested_obj")

    val (rewrittenSchemaSubset, schemaSubset) = getInputAndOutputWhyNotTuple(res, whyNotTupleProjectionNewName("nested_obj"))

    assert(schemaSubset.rootNode.name == rewrittenSchemaSubset.rootNode.name)
    assert(rewrittenSchemaSubset.rootNode.children.head.name == "nested_obj")
  }

  test("[Unrestructure] Nested attribute is renamed"){
    val newName = "renamed"
    val df = getDataFrame()
    val res = df.select($"nested_obj.nested_obj".alias(newName))

    val (rewrittenSchemaSubset, schemaSubset) = getInputAndOutputWhyNotTuple(res, whyNotTupleProjectionNewName(newName))

    assert(schemaSubset.rootNode.name == rewrittenSchemaSubset.rootNode.name)
    assert(rewrittenSchemaSubset.rootNode.children.head.name == newName)
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

    val (rewrittenSchemaSubset, schemaSubset) = getInputAndOutputWhyNotTuple(res, whyNotQuestionWithNesting2(newName1 ))


    val flat_key = rewrittenSchemaSubset.rootNode.children.find(node => node.name == "flat_key").getOrElse(fail("flat_key not where it is supposed to be"))
    val nested_obj = rewrittenSchemaSubset.rootNode.children.headOption.getOrElse(fail("nested_obj not where it is supposed to be"))

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

  test("[Unrestructure] Create multiple attributes in multiple structures"){
    val newName1 = "tupleOne"
    val newName2 = "tupleTwo"
    val df = getDataFrame()
    val res = df.select(struct($"flat_key", struct($"nested_obj").alias(newName2)).alias(newName1))


    val (rewrittenSchemaSubset, schemaSubset) = getInputAndOutputWhyNotTuple(res, whyNotQuestionWithNesting3(newName1, newName2))


    val flat_key = rewrittenSchemaSubset.rootNode.children.find(node => node.name == "flat_key").getOrElse(fail("flat_key not where it is supposed to be"))
    val nested_obj = rewrittenSchemaSubset.rootNode.children.headOption.getOrElse(fail("nested_obj not where it is supposed to be"))

    assert(flat_key.name == "flat_key")
    assert(nested_obj.name == "nested_obj")
  }

  test("[Unrestructure] Create multiple attributes in multiple structures and multiple unnesting"){
    val newName1 = "tupleOne"
    val newName2 = "tupleTwo"
    val df = getDataFrame()
    val res = df.select(struct($"flat_key", struct($"nested_obj.nested_obj").alias(newName2)).alias(newName1))


    val (rewrittenSchemaSubset, schemaSubset) = getInputAndOutputWhyNotTuple(res, whyNotQuestionWithNesting3(newName1, newName2))


    val flat_key = rewrittenSchemaSubset.rootNode.children.find(node => node.name == "flat_key").getOrElse(fail("flat_key not where it is supposed to be"))
    val nested_obj1 = rewrittenSchemaSubset.rootNode.children.headOption.getOrElse(fail("nested_obj not where it is supposed to be"))
    val nested_obj2 = nested_obj1.children.headOption.getOrElse(fail("nested_obj not where it is supposed to be"))


    assert(flat_key.name == "flat_key")
    assert(nested_obj1.name == "nested_obj")
    assert(nested_obj2.name == "nested_obj")
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


    val nested_obj1 = rewrittenSchemaSubset.rootNode.children.headOption.getOrElse(fail("nested_obj not where it is supposed to be"))
    val nested_obj2 = nested_obj1.children.headOption.getOrElse(fail("nested_obj not where it is supposed to be"))
    val size1 = rewrittenSchemaSubset.rootNode.children.size
    val size2 = rewrittenSchemaSubset.rootNode.children.size

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
    val nested_obj1 = rewrittenSchemaSubset.rootNode.children.headOption.getOrElse(fail("nested_obj not where it is supposed to be"))
    val size = rewrittenSchemaSubset.rootNode.children.size


    assert(flat_key.name == "flat_key")
    assert(size == 1)

  }

















}