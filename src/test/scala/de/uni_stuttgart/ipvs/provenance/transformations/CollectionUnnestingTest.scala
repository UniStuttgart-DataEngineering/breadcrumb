package de.uni_stuttgart.ipvs.provenance.transformations

import com.github.mrpowers.spark.fast.tests.{ColumnComparer, DataFrameComparer}
import de.uni_stuttgart.ipvs.provenance.SharedSparkTestDataFrames
import de.uni_stuttgart.ipvs.provenance.nested_why_not.{Constants, WhyNotProvenance}
import de.uni_stuttgart.ipvs.provenance.schema_alternatives.SchemaSubsetTree
import de.uni_stuttgart.ipvs.provenance.why_not_question.{Schema, Twig}
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.catalyst.plans.logical.{Generate, Project}
import org.apache.spark.sql.functions.explode
import org.scalatest.FunSuite

class CollectionUnnestingTest extends FunSuite with SharedSparkTestDataFrames with DataFrameComparer with ColumnComparer{

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
      val df = getDataFrame(pathToNestedData0)
      val otherDf = df.withColumn("flattened", explode($"nested_list"))
      val res = WhyNotProvenance.rewrite(otherDf, nestedWhyNotTuple)
      assert(res.schema.filter(field => field.name.contains(Constants.SURVIVED_FIELD)).size == 1)
    }

  test("[Rewrite] Explode contains compatible field") {
    val df = getDataFrame(pathToNestedData0)
    val otherDf = df.withColumn("flattened", explode($"nested_list"))
    val res = WhyNotProvenance.rewrite(otherDf, nestedWhyNotTuple)
    assert(res.schema.filter(field => field.name.contains(Constants.COMPATIBLE_FIELD)).size == 2)
  }

  test("[Rewrite] Explode retains empty collection") {
    val df = getDataFrame(pathToNestedData00)
    val otherDf = df.withColumn("flattened", explode($"nested_list"))
    val res = WhyNotProvenance.rewrite(otherDf, nestedWhyNotTuple)
    assert(res.filter($"flat_key" === "6_flat_val_y").count() == 1)
  }

  test("[Rewrite] Explode retains element with null value") {
    val df = getDataFrame(pathToNestedData00)
    val otherDf = df.withColumn("flattened", explode($"nested_list"))
    val res = WhyNotProvenance.rewrite(otherDf, nestedWhyNotTuple)
    assert(res.filter($"flat_key" === "5_flat_val_y").count() == 1)
  }


  def getInputAndOutputWhyNotTuple(outputDataFrame: DataFrame, outputWhyNotTuple: Twig): (SchemaSubsetTree, SchemaSubsetTree) = {
    val schemaMatch = getSchemaMatch(outputDataFrame, outputWhyNotTuple)
    val schemaSubset = SchemaSubsetTree(schemaMatch, new Schema(outputDataFrame))
    val rewrite = ProjectRewrite(outputDataFrame.queryExecution.analyzed.asInstanceOf[Project], schemaSubset, 1)

    (rewrite.unrestructure(), schemaSubset) // (inputWhyNotTuple, outputWhyNotTuple)
  }


  def whyNotTupleUnnested(): Twig = {
    var twig = new Twig()
    val root = twig.createNode("root", 1, 1, "")
    val element1 = twig.createNode("flattened", 1, 1, "1_list_val_1_1")
    twig = twig.createEdge(root, element1, false)
    twig.validate().get
  }


  test("[Unrestructure] Explode a single element") {
    val df = getDataFrame(pathToNestedData0)
    val res = df.withColumn("flattened", explode($"nested_list"))
//    res.printSchema()

    val (rewrittenSchemaSubset, schemaSubset) = getInputAndOutputWhyNotTuple(res, whyNotTupleUnnested())

    assert(schemaSubset.rootNode.name == rewrittenSchemaSubset.rootNode.name)
    assert(rewrittenSchemaSubset.rootNode.children.head.name == "nested_list")
    assert(rewrittenSchemaSubset.rootNode.children.head.children.head.name == "element")
  }


  def whyNotTupleUnnestedMultiElem(): Twig = {
    var twig = new Twig()
    val root = twig.createNode("root", 1, 1, "")
    val struct1 = twig.createNode("flattened", 1, 1, "")
    val element1 = twig.createNode("id_str", 1, 1, "")
    val element2 = twig.createNode("name", 1, 1, "Lisa Paul")
    twig = twig.createEdge(root, struct1, false)
    twig = twig.createEdge(struct1, element1, false)
    twig = twig.createEdge(struct1, element2, false)
    twig.validate().get
  }


  test("[Unrestructure] Explode multiple elements") {
    val df = getDataFrame(pathToDemoData)
    val res = df.withColumn("flattened", explode($"user_mentions"))
//    res.printSchema()

    val (rewrittenSchemaSubset, schemaSubset) = getInputAndOutputWhyNotTuple(res, whyNotTupleUnnestedMultiElem())

    assert(schemaSubset.rootNode.name == rewrittenSchemaSubset.rootNode.name)
    assert(rewrittenSchemaSubset.rootNode.children.head.name == "user_mentions")
    assert(rewrittenSchemaSubset.rootNode.children.head.children.head.name == "element")

    val id_str = rewrittenSchemaSubset.rootNode.children.head.children.head.children.find(node => node.name == "id_str").getOrElse(fail("id_str not where it is supposed to be"))
    val name = rewrittenSchemaSubset.rootNode.children.head.children.head.children.find(node => node.name == "name").getOrElse(fail("name not where it is supposed to be"))
    assert(id_str.name == "id_str")
    assert(name.name == "name")
  }


  def whyNotTupleUnnestedMultiElem2(): Twig = {
    var twig = new Twig()
    val root = twig.createNode("root", 1, 1, "")
    val struct1 = twig.createNode("flattened", 1, 1, "")
    //    val element1 = twig.createNode("id_str", 1, 1, "")
    val element2 = twig.createNode("name", 1, 1, "Lisa Paul")
    twig = twig.createEdge(root, struct1, false)
    //    twig = twig.createEdge(struct1, element1, false)
    twig = twig.createEdge(struct1, element2, false)
    twig.validate().get
  }


  test("[Unrestructure] Explode multiple elements but reference only one attribute") {
    val df = getDataFrame(pathToDemoData)
    val res = df.withColumn("flattened", explode($"user_mentions"))
    //    res.printSchema()

    val (rewrittenSchemaSubset, schemaSubset) = getInputAndOutputWhyNotTuple(res, whyNotTupleUnnestedMultiElem2())

    assert(schemaSubset.rootNode.name == rewrittenSchemaSubset.rootNode.name)
    assert(rewrittenSchemaSubset.rootNode.children.head.name == "user_mentions")
    assert(rewrittenSchemaSubset.rootNode.children.head.children.head.name == "element")

//    val id_str = rewrittenSchemaSubset.rootNode.children.head.children.head.children.find(node => node.name == "id_str").getOrElse(fail("id_str not where it is supposed to be"))
    val name = rewrittenSchemaSubset.rootNode.children.head.children.head.children.find(node => node.name == "name").getOrElse(fail("name not where it is supposed to be"))
//    assert(id_str.name == "id_str")
    assert(name.name == "name")
  }


  def whyNotTupleUnnestedMultiElemRenamed(): Twig = {
    var twig = new Twig()
    val root = twig.createNode("root", 1, 1, "")
    val struct1 = twig.createNode("address1", 1, 1, "")
    val element1 = twig.createNode("city", 1, 1, "LA")
    val element2 = twig.createNode("year", 1, 1, "")
    twig = twig.createEdge(root, struct1, false)
    twig = twig.createEdge(struct1, element1, false)
    twig = twig.createEdge(struct1, element2, false)
    twig.validate().get
  }


  test("[Unrestructure] Explode multiple elements without renaming") {
    val df = getDataFrame(pathToExampleData)
    val res = df.withColumn("address1", explode($"address1"))
//    res.printSchema()

    val (rewrittenSchemaSubset, schemaSubset) = getInputAndOutputWhyNotTuple(res, whyNotTupleUnnestedMultiElemRenamed())

    assert(schemaSubset.rootNode.name == rewrittenSchemaSubset.rootNode.name)
    assert(rewrittenSchemaSubset.rootNode.children.head.name == "address1")
    assert(rewrittenSchemaSubset.rootNode.children.head.children.head.name == "element")

    val city = rewrittenSchemaSubset.rootNode.children.head.children.head.children.find(node => node.name == "city").getOrElse(fail("city not where it is supposed to be"))
    val year = rewrittenSchemaSubset.rootNode.children.head.children.head.children.find(node => node.name == "year").getOrElse(fail("year not where it is supposed to be"))
    assert(city.name == "city")
    assert(year.name == "year")
  }


  def whyNotTupleUnnestedMultiElem3(): Twig = {
    var twig = new Twig()
    val root = twig.createNode("root", 1, 1, "")
    val struct1 = twig.createNode("flattened_address1", 1, 1, "")
    val element1 = twig.createNode("city", 1, 1, "LA")
    val element2 = twig.createNode("year", 1, 1, "")
    val struct2 = twig.createNode("flattened_address2", 1, 1, "")
    val element3 = twig.createNode("city", 1, 1, "LA")
    val element4 = twig.createNode("year", 1, 1, "")
    twig = twig.createEdge(root, struct1, false)
    twig = twig.createEdge(struct1, element1, false)
    twig = twig.createEdge(struct1, element2, false)
    twig = twig.createEdge(root, struct2, false)
    twig = twig.createEdge(struct2, element3, false)
    twig = twig.createEdge(struct2, element4, false)
    twig.validate().get
  }


  test("[Unrestructure] Explode multiple collections") {
    val df = getDataFrame(pathToExampleData)
    val res = df.withColumn("flattened_address1", explode($"address1")).withColumn("flattened_address2", explode($"address2"))
//    res = res.withColumn("flattened_address2", explode($"address2"))
//    res.printSchema()

    val (rewrittenSchemaSubset, schemaSubset) = getInputAndOutputWhyNotTuple(res, whyNotTupleUnnestedMultiElem3())

    assert(schemaSubset.rootNode.name == rewrittenSchemaSubset.rootNode.name)
    val address1 = rewrittenSchemaSubset.rootNode.children.find(node => node.name == "address1").getOrElse(fail("address1 not where it is supposed to be"))
    val address2 = rewrittenSchemaSubset.rootNode.children.find(node => node.name == "address2").getOrElse(fail("address2 not where it is supposed to be"))
    val city1 = rewrittenSchemaSubset.rootNode.children.head.children.head.children.find(node => node.name == "city").getOrElse(fail("city1 not where it is supposed to be"))
    val year1 = rewrittenSchemaSubset.rootNode.children.head.children.head.children.find(node => node.name == "year").getOrElse(fail("year1 not where it is supposed to be"))
    val city2 = rewrittenSchemaSubset.rootNode.children.last.children.head.children.find(node => node.name == "city").getOrElse(fail("city2 not where it is supposed to be"))
    val year2 = rewrittenSchemaSubset.rootNode.children.last.children.head.children.find(node => node.name == "year").getOrElse(fail("year2 not where it is supposed to be"))

    assert(address1.name == "address1")
    assert(address2.name == "address2")
    assert(city1.name == "city")
    assert(year1.name == "year")
    assert(city2.name == "city")
    assert(year2.name == "year")
  }


  test("[Unrestructure] Explode multiple collections simultaneously") {
    val df = getDataFrame(pathToExampleData)
    df.show(false)

//    val res = df.withColumn("addresses", explode(arrays_zip($"address1", $"address2"))).select($"name", $"vars.varA", $"vars.varB")
//    res.show(false)
  }
}
