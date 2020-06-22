package de.uni_stuttgart.ipvs.provenance.transformations

import com.github.mrpowers.spark.fast.tests.{ColumnComparer, DataFrameComparer}
import de.uni_stuttgart.ipvs.provenance.SharedSparkTestDataFrames
import de.uni_stuttgart.ipvs.provenance.nested_why_not.{Constants, WhyNotProvenance}
import de.uni_stuttgart.ipvs.provenance.schema_alternatives.SchemaSubsetTree
import de.uni_stuttgart.ipvs.provenance.why_not_question.{Schema, Twig}
import org.apache.spark.sql.catalyst.plans.logical.{Filter, LeafNode}
import org.scalatest.FunSuite

class FilterTest extends FunSuite with SharedSparkTestDataFrames with DataFrameComparer with ColumnComparer{

  import spark.implicits._

  test("[Rewrite] Filter adds survived column") {
    val df = singleInputColumnDataFrame()
    val otherDf = df.filter($"MyIntCol" > 2)
    val res = WhyNotProvenance.rewrite(otherDf, myIntColWhyNotQuestion())
    assert(res.schema.filter(field => field.name.contains(Constants.SURVIVED_FIELD)).size == 1)
  }

  test("[Rewrite] Rewritten filter marks all items that do not survive the original filter with false") {
    val df = singleInputColumnDataFrame()
    val otherDf = df.filter($"MyIntCol" > 2)
    val res = WhyNotProvenance.rewrite(otherDf, myIntColWhyNotQuestion())
    val survivedColumn = res.schema.find(field => field.name.contains(Constants.SURVIVED_FIELD)).getOrElse(fail("SURVIVED FIELD NOT FOUND"))
    val filteredRes = res.filter(res.col(survivedColumn.name) === true).select($"MyIntCol")
    filteredRes.explain(true)
    assertSmallDataFrameEquality(otherDf, filteredRes)
  }

  test("[Rewrite] Filter copies the compatible column") {
    val df = singleInputColumnDataFrame()
    val otherDf = df.filter($"MyIntCol" > 2)
    val res = WhyNotProvenance.rewrite(otherDf, myIntColWhyNotQuestion())
    val compatibleFields = res.schema.filter(field => field.name.contains(Constants.COMPATIBLE_FIELD))
    assert(compatibleFields.size == 2)
    assertColumnEquality(res, compatibleFields(0).name, compatibleFields(1).name)
  }


  def whyNotTupleFilterNewName(newName: String): Twig = {
    var twig = new Twig()
    val root = twig.createNode("root", 1, 1, "")
    val user = twig.createNode(newName, 1, 1, "")
    twig = twig.createEdge(root, user, false)
    twig.validate().get
  }


  def whyNotTupleFilterNewName2(newName: String, newSubName: String): Twig = {
    var twig = new Twig()
    val root = twig.createNode("root", 1, 1, "")
    val user = twig.createNode(newName, 1, 1, "")
    val id = twig.createNode(newSubName, 1, 1, "")
    twig = twig.createEdge(root, user, false)
    twig = twig.createEdge(user, id, false)
    twig.validate().get
  }


  def whyNotTupleFilterNewName3(newName: String, newSubName1: String, newSubName2: String): Twig = {
    var twig = new Twig()
    val root = twig.createNode("root", 1, 1, "")
    val user = twig.createNode(newName, 1, 1, "")
    val id = twig.createNode(newSubName1, 1, 1, "")
    val userName = twig.createNode(newSubName2, 1, 1, "")
    twig = twig.createEdge(root, user, false)
    twig = twig.createEdge(user, id, false)
    twig = twig.createEdge(user, userName, false)
    twig.validate().get
  }


  test("[Unrestructure] Filter over nested tuple") {
    //    val newName = "renamed"
    val df = getDataFrame(pathToDemoData1)
    val res = df.filter($"user.name" === "Lisa Paul")

    val schemaMatch = getSchemaMatch(res, whyNotTupleFilterNewName("user"))
    val schemaSubset = SchemaSubsetTree(schemaMatch, new Schema(res))

    val plan = res.queryExecution.analyzed
    val rewrittenSchemaSubset1 = FilterRewrite(plan.asInstanceOf[Filter], schemaSubset, 1).unrestructure()
    val rewrittenSchemaSubset2 = RelationRewrite(plan.children.head.asInstanceOf[LeafNode], rewrittenSchemaSubset1, 1).unrestructure()

    assert(schemaSubset.rootNode.name == rewrittenSchemaSubset1.rootNode.name)
    assert(schemaSubset.rootNode.name == rewrittenSchemaSubset2.rootNode.name)

    val id = rewrittenSchemaSubset2.rootNode.children.head.children.find(node => node.name == "id_str").getOrElse(fail("id_str not where it is supposed to be"))
    val userName = rewrittenSchemaSubset2.rootNode.children.head.children.find(node => node.name == "name").getOrElse(fail("name not where it is supposed to be"))

    assert(rewrittenSchemaSubset2.rootNode.children.head.name == "user")
    assert(id.name == "id_str")
    assert(userName.name == "name")
  }


  test("[Unrestructure] Filter over nested tuple but reference one attribute") {
    //    val newName = "renamed"
    val df = getDataFrame(pathToDemoData1)
    val res = df.filter($"user.name" === "Lisa Paul")

    val schemaMatch = getSchemaMatch(res, whyNotTupleFilterNewName2("user", "id_str"))
    val schemaSubset = SchemaSubsetTree(schemaMatch, new Schema(res))

    val plan = res.queryExecution.analyzed
    val rewrittenSchemaSubset1 = FilterRewrite(plan.asInstanceOf[Filter], schemaSubset, 1).unrestructure()
    val rewrittenSchemaSubset2 = RelationRewrite(plan.children.head.asInstanceOf[LeafNode], rewrittenSchemaSubset1, 1).unrestructure()

    assert(schemaSubset.rootNode.name == rewrittenSchemaSubset1.rootNode.name)
    assert(schemaSubset.rootNode.name == rewrittenSchemaSubset2.rootNode.name)

    val id = rewrittenSchemaSubset2.rootNode.children.head.children.find(node => node.name == "id_str").getOrElse(fail("id_str not where it is supposed to be"))
//    val userName = rewrittenSchemaSubset2.rootNode.children.head.children.find(node => node.name == "name").getOrElse(fail("name not where it is supposed to be"))

    assert(rewrittenSchemaSubset2.rootNode.children.head.name == "user")
    assert(id.name == "id_str")
//    assert(userName.name == "name")

    val size = rewrittenSchemaSubset2.rootNode.children.head.children.size
    assert(size == 1)
  }


  test("[Unrestructure] Filter over nested tuple 2") {
    //    val newName = "renamed"
    val df = getDataFrame(pathToDemoData1)
    val res = df.filter($"user.name" === "Lisa Paul")

    val schemaMatch = getSchemaMatch(res, whyNotTupleFilterNewName3("user", "id_str", "name"))
    val schemaSubset = SchemaSubsetTree(schemaMatch, new Schema(res))

    val plan = res.queryExecution.analyzed
    val rewrittenSchemaSubset1 = FilterRewrite(plan.asInstanceOf[Filter], schemaSubset, 1).unrestructure()
    val rewrittenSchemaSubset2 = RelationRewrite(plan.children.head.asInstanceOf[LeafNode], rewrittenSchemaSubset1, 1).unrestructure()

    assert(schemaSubset.rootNode.name == rewrittenSchemaSubset1.rootNode.name)
    assert(schemaSubset.rootNode.name == rewrittenSchemaSubset2.rootNode.name)

    val id = rewrittenSchemaSubset2.rootNode.children.head.children.find(node => node.name == "id_str").getOrElse(fail("id_str not where it is supposed to be"))
    val userName = rewrittenSchemaSubset2.rootNode.children.head.children.find(node => node.name == "name").getOrElse(fail("name not where it is supposed to be"))

    assert(rewrittenSchemaSubset2.rootNode.children.head.name == "user")
    assert(id.name == "id_str")
    assert(userName.name == "name")
  }


}
