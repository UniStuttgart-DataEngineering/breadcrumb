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

  def whyNotTupleProjection1(): Twig = {
    var twig = new Twig()
    val root = twig.createNode("root", 1, 1, "")
    val flat_key = twig.createNode("flat_key", 1, 1, "")
    twig = twig.createEdge(root, flat_key, false)
    twig.validate().get
  }

  def whyNotTupleProjection2(): Twig = {
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
    val schemaMatch = getSchemaMatch(df, whyNotTupleProjection1())
    val schemaSubset = SchemaSubsetTree(schemaMatch, new Schema(df))
    val res = df.select($"flat_key")

    val rewrite = ProjectRewrite(res.queryExecution.analyzed.asInstanceOf[Project],schemaSubset, 1)
    val rewrittenSchemaSubset = rewrite.unrestructure()
    assert(schemaSubset.rootNode.name == rewrittenSchemaSubset.rootNode.name)
    assert(schemaSubset.rootNode.children.head.name == rewrittenSchemaSubset.rootNode.children.head.name)
  }

  test("[Unrestructure] Attribute is renamed"){
    val newName = "renamed"
    val df = getDataFrame()
    val schemaMatch = getSchemaMatch(df, whyNotTupleProjection1())
    val schemaSubset = SchemaSubsetTree(schemaMatch, new Schema(df))
    val res = df.select($"flat_key".alias(newName))

    val rewrite = ProjectRewrite(res.queryExecution.analyzed.asInstanceOf[Project],schemaSubset, 1)
    val rewrittenSchemaSubset = rewrite.unrestructure()
    assert(schemaSubset.rootNode.name == rewrittenSchemaSubset.rootNode.name)
    assert(rewrittenSchemaSubset.rootNode.children.head.name == newName)
  }

  test("[Unrestructure] Nested attribute is not renamed"){
    val df = getDataFrame()
    val schemaMatch = getSchemaMatch(df, whyNotTupleProjection2())
    val schemaSubset = SchemaSubsetTree(schemaMatch, new Schema(df))
    val res = df.select($"nested_obj.nested_obj")
    val rewrite = ProjectRewrite(res.queryExecution.analyzed.asInstanceOf[Project],schemaSubset, 1)
    val rewrittenSchemaSubset = rewrite.unrestructure()
    assert(schemaSubset.rootNode.name == rewrittenSchemaSubset.rootNode.name)
    assert(rewrittenSchemaSubset.rootNode.children.head.name == "nested_obj")
  }

  test("[Unrestructure] Nested attribute is renamed"){
    val newName = "renamed"
    val df = getDataFrame()
    val schemaMatch = getSchemaMatch(df, whyNotTupleProjection2())
    val schemaSubset = SchemaSubsetTree(schemaMatch, new Schema(df))
    val res = df.select($"nested_obj.nested_obj".alias("nonNestedObj"))
    val rewrite = ProjectRewrite(res.queryExecution.analyzed.asInstanceOf[Project],schemaSubset, 1)
    val rewrittenSchemaSubset = rewrite.unrestructure()
    assert(schemaSubset.rootNode.name == rewrittenSchemaSubset.rootNode.name)
    assert(rewrittenSchemaSubset.rootNode.children.head.name == newName)
  }

  test("[Unrestructure] Flat attribute is nested"){
    val newName = "tupleName"
    val df = getDataFrame()
    val schemaMatch = getSchemaMatch(df, whyNotTupleProjection2())
    val schemaSubset = SchemaSubsetTree(schemaMatch, new Schema(df))
    val res = df.select(struct($"flat_key").alias(newName))
    val rewrite = ProjectRewrite(res.queryExecution.analyzed.asInstanceOf[Project],schemaSubset, 1)
    val rewrittenSchemaSubset = rewrite.unrestructure()

    val tuple1 = rewrittenSchemaSubset.rootNode.children.headOption.getOrElse(fail("tuple 1 not where it is supposed to be"))
    val flat_key = tuple1.children.find(node => node.name == "flat_key").getOrElse(fail("flat_key not where it is supposed to be"))

    assert(schemaSubset.rootNode.name == rewrittenSchemaSubset.rootNode.name)
    assert(tuple1.name == newName)
    assert(flat_key.name == "flat_key")

  }

  test("[Unrestructure] Create multiple attributes in a structure"){
    val newName1 = "tupleOne"
    val df = getDataFrame()
    val schemaMatch = getSchemaMatch(df, whyNotTupleProjection2())
    val schemaSubset = SchemaSubsetTree(schemaMatch, new Schema(df))
    val res = df.select(struct($"flat_key", $"nested_obj").alias(newName1))
    val rewrite = ProjectRewrite(res.queryExecution.analyzed.asInstanceOf[Project],schemaSubset, 1)
    val rewrittenSchemaSubset = rewrite.unrestructure()

    val tuple1 = rewrittenSchemaSubset.rootNode.children.headOption.getOrElse(fail("tuple 1 not where it is supposed to be"))
    val flat_key = tuple1.children.find(node => node.name == "flat_key").getOrElse(fail("flat_key not where it is supposed to be"))
    val nested_obj = tuple1.children.headOption.getOrElse(fail("nested_obj not where it is supposed to be"))

    assert(tuple1.name == newName1)
    assert(flat_key.name == "flat_key")
    assert(nested_obj.name == "nested_obj")
  }

  test("[Unrestructure] Create multiple attributes in multiple structures"){
    val newName1 = "tupleOne"
    val newName2 = "tupleTwo"
    val df = getDataFrame()
    val schemaMatch = getSchemaMatch(df, whyNotTupleProjection2())
    val schemaSubset = SchemaSubsetTree(schemaMatch, new Schema(df))
    val res = df.select(struct($"flat_key", struct($"nested_obj").alias(newName1)).alias(newName2))
    val rewrite = ProjectRewrite(res.queryExecution.analyzed.asInstanceOf[Project],schemaSubset, 1)
    val rewrittenSchemaSubset = rewrite.unrestructure()


    val tuple1 = rewrittenSchemaSubset.rootNode.children.headOption.getOrElse(fail("tuple 1 not where it is supposed to be"))
    val flat_key = tuple1.children.find(node => node.name == "flat_key").getOrElse(fail("flat_key not where it is supposed to be"))
    val tuple2 = tuple1.children.find(node => node.name == newName2).getOrElse(fail("tuple 2 not where it is supposed to be"))
    val nested_obj = tuple2.children.headOption.getOrElse(fail("nested_obj not where it is supposed to be"))

    assert(tuple1.name == newName1)
    assert(tuple2.name == newName2)
    assert(flat_key.name == "flat_key")
    assert(nested_obj.name == "nested_obj")
  }

















}