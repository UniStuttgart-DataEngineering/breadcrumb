package de.uni_stuttgart.ipvs.provenance.transformations

import com.github.mrpowers.spark.fast.tests.{ColumnComparer, DataFrameComparer}
import de.uni_stuttgart.ipvs.provenance.SharedSparkTestDataFrames
import de.uni_stuttgart.ipvs.provenance.nested_why_not.{Constants, WhyNotProvenance}
import de.uni_stuttgart.ipvs.provenance.schema_alternatives.SchemaSubsetTree
import de.uni_stuttgart.ipvs.provenance.why_not_question.{Schema, SchemaBackTrace, Twig}
import org.apache.spark.sql.DataFrame
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


//  def getInputAndOutputWhyNotTuple(outputDataFrame: DataFrame, outputWhyNotTuple: Twig): (SchemaSubsetTree, SchemaSubsetTree, SchemaSubsetTree) = {
//    val schemaMatch = getSchemaMatch(outputDataFrame, outputWhyNotTuple)
//    val schemaSubset = SchemaSubsetTree(schemaMatch, new Schema(outputDataFrame))
////    val rewrite = SchemaBackTrace(outputDataFrame.queryExecution.analyzed, schemaSubset)
////
////    (rewrite.unrestructure().head, schemaSubset) // (inputWhyNotTuple, outputWhyNotTuple)
//
//    val rewrite = FilterRewrite(outputDataFrame.queryExecution.analyzed.asInstanceOf[Filter], -1)
//    val rewrite2 = RelationRewrite(rewrite.child.plan.asInstanceOf[LeafNode], -1)
//
//    (rewrite.undoSchemaModifications(schemaSubset), rewrite2.undoSchemaModifications(schemaSubset), schemaSubset)
//  }


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

    val plan = res.queryExecution.analyzed
    val schemaSubset = getSchemaSubsetTree(res, whyNotTupleFilterNewName("user"))
    val rewrittenSchemaSubsetFilter = getInputAndOutputWhyNotTupleFlex(plan, schemaSubset, "")
    val rewrittenSchemaSubsetRelation = getInputAndOutputWhyNotTupleFlex(plan.children.head, rewrittenSchemaSubsetFilter, "")

//    val (rewrittenSchemaSubsetFilter, rewrittenSchemaSubsetRelation, schemaSubset) = getInputAndOutputWhyNotTuple(res, whyNotTupleFilterNewName("user"))

    assert(schemaSubset.rootNode.name == rewrittenSchemaSubsetFilter.rootNode.name)
    assert(schemaSubset.rootNode.name == rewrittenSchemaSubsetRelation.rootNode.name)

//    val id = rewrittenSchemaSubsetRelation.rootNode.children.head.children.find(node => node.name == "id_str").getOrElse(fail("id_str not where it is supposed to be"))
//    val userName = rewrittenSchemaSubsetRelation.rootNode.children.head.children.find(node => node.name == "name").getOrElse(fail("name not where it is supposed to be"))

    assert(rewrittenSchemaSubsetRelation.rootNode.children.head.name == "user")
//    assert(id.name == "id_str")
//    assert(userName.name == "name")
  }


  test("[Unrestructure] Filter over nested tuple but reference one attribute") {
    //    val newName = "renamed"
    val df = getDataFrame(pathToDemoData1)
    val res = df.filter($"user.name" === "Lisa Paul")

    val plan = res.queryExecution.analyzed
    val schemaSubset = getSchemaSubsetTree(res, whyNotTupleFilterNewName2("user", "id_str"))
    val rewrittenSchemaSubsetFilter = getInputAndOutputWhyNotTupleFlex(plan, schemaSubset, "")
    val rewrittenSchemaSubsetRelation = getInputAndOutputWhyNotTupleFlex(plan.children.head, rewrittenSchemaSubsetFilter, "")

//    val (rewrittenSchemaSubsetFilter, rewrittenSchemaSubsetRelation, schemaSubset) =
//      getInputAndOutputWhyNotTuple(res, whyNotTupleFilterNewName2("user", "id_str"))

    assert(schemaSubset.rootNode.name == rewrittenSchemaSubsetFilter.rootNode.name)
    assert(schemaSubset.rootNode.name == rewrittenSchemaSubsetRelation.rootNode.name)
    assert(rewrittenSchemaSubsetRelation.rootNode.children.head.name == "user")

    val id = rewrittenSchemaSubsetRelation.rootNode.children.head.children.find(node => node.name == "id_str").getOrElse(fail("id_str not where it is supposed to be"))
    assert(id.name == "id_str")

    val size = rewrittenSchemaSubsetRelation.rootNode.children.head.children.size
    assert(size == 1)
  }


  test("[Unrestructure] Filter over nested tuple 2") {
    //    val newName = "renamed"
    val df = getDataFrame(pathToDemoData1)
    val res = df.filter($"user.name" === "Lisa Paul")

    val plan = res.queryExecution.analyzed
    val schemaSubset = getSchemaSubsetTree(res, whyNotTupleFilterNewName3("user", "id_str", "name"))
    val rewrittenSchemaSubsetFilter = getInputAndOutputWhyNotTupleFlex(plan, schemaSubset, "")
    val rewrittenSchemaSubsetRelation = getInputAndOutputWhyNotTupleFlex(plan.children.head, rewrittenSchemaSubsetFilter, "")

//    val (rewrittenSchemaSubsetFilter, rewrittenSchemaSubsetRelation, schemaSubset) =
//      getInputAndOutputWhyNotTuple(res, whyNotTupleFilterNewName3("user", "id_str", "name"))

    assert(schemaSubset.rootNode.name == rewrittenSchemaSubsetFilter.rootNode.name)
    assert(schemaSubset.rootNode.name == rewrittenSchemaSubsetRelation.rootNode.name)

    val id = rewrittenSchemaSubsetRelation.rootNode.children.head.children.find(node => node.name == "id_str").getOrElse(fail("id_str not where it is supposed to be"))
    val userName = rewrittenSchemaSubsetRelation.rootNode.children.head.children.find(node => node.name == "name").getOrElse(fail("name not where it is supposed to be"))

    assert(rewrittenSchemaSubsetRelation.rootNode.children.head.name == "user")
    assert(id.name == "id_str")
    assert(userName.name == "name")
  }


  def whyNotTupleAlternatives(): Twig = {
    var twig = new Twig()
    val root = twig.createNode("root", 1, 1, "")
    val key = twig.createNode("key", 1, 1, "1")
    val value = twig.createNode("value", 1, 1, "2")

    twig = twig.createEdge(root, key, false)
    twig = twig.createEdge(root, value, false)
    twig.validate().get
  }


  test("[ProvenanceWithSchemaAlternatives]  Initial alternatives test") {
    val df = getDataFrame(pathToSchemaAlternative)
    val otherDf = df.filter($"value" < 101)
    val res = WhyNotProvenance.rewriteWithAlternatives(otherDf, whyNotTupleAlternatives())
    res.show()
  }



}
