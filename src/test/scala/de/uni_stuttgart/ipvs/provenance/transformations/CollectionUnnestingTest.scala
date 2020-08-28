package de.uni_stuttgart.ipvs.provenance.transformations

import com.github.mrpowers.spark.fast.tests.{ColumnComparer, DataFrameComparer}
import de.uni_stuttgart.ipvs.provenance.SharedSparkTestDataFrames
import de.uni_stuttgart.ipvs.provenance.nested_why_not.{Constants, ProvenanceContext, Rewrite, WhyNotPlanRewriter, WhyNotProvenance}
import de.uni_stuttgart.ipvs.provenance.schema_alternatives.SchemaSubsetTree
import de.uni_stuttgart.ipvs.provenance.why_not_question.{Schema, SchemaBackTrace, Twig}
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.catalyst.expressions.Literal
import org.apache.spark.sql.catalyst.plans.logical.{Generate, Project}
import org.apache.spark.sql.functions.{element_at, explode, greatest, posexplode_outer, size, typedLit, udf, when}
import org.apache.spark.sql.types.ArrayType
import org.scalatest.FunSuite

class CollectionUnnestingTest extends FunSuite with SharedSparkTestDataFrames with DataFrameComparer with ColumnComparer{

    import spark.implicits._

    def nestedWhyNotTuple(): Twig = {
      var twig = new Twig()
      val root = twig.createNode("root", 1, 1, "")
//      val nested_list = twig.createNode("nested_list", 1, 1, "")
      val nested_list = twig.createNode("flattened", 1, 1, "1_list_val_1_1")
//      val element1 = twig.createNode("element", 1, 1, "1_list_val_1_1")
      twig = twig.createEdge(root, nested_list, false)
//      twig = twig.createEdge(nested_list, element1, false)
      twig.validate().get
    }

    test("[Rewrite] Explode contains survived field") {
      val df = getDataFrame(pathToNestedData0)
      var otherDf = df.withColumn("flattened", explode($"nested_list"))
      val res = WhyNotProvenance.rewrite(otherDf, nestedWhyNotTuple)
      res.show(false)
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
//    val rewrite = ProjectRewrite(outputDataFrame.queryExecution.analyzed.asInstanceOf[Project], schemaSubset, 1)
    //val rewrite = SchemaBackTrace(outputDataFrame.queryExecution.analyzed, schemaSubset)

    val rewrite = GenerateRewrite(outputDataFrame.queryExecution.analyzed.children.head.asInstanceOf[Generate], -1)
    (rewrite.undoSchemaModifications(schemaSubset), schemaSubset)
    //(rewrite.unrestructure().head, schemaSubset) // (inputWhyNotTuple, outputWhyNotTuple)
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

    val (rewrittenSchemaSubset2, schemaSubset) = getInputAndOutputWhyNotTuple(res, whyNotTupleUnnestedMultiElem3())
    val grandChild = res.queryExecution.analyzed.children.head.children.head
    val rewrittenSchemaSubset = GenerateRewrite(grandChild.children.head.asInstanceOf[Generate], -1).undoSchemaModifications(rewrittenSchemaSubset2)

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

  test("[Exploration] Get size of nested array") {
    val df = getDataFrame(pathToExampleDataFlatten)
    var otherDf = df.withColumn("size_address1", size($"address1"))
    otherDf = otherDf.withColumn("size_address2", size($"address2"))
    otherDf = otherDf.select(otherDf.col("*"), posexplode_outer($"address2").as(Seq("p2", "a2")))
    otherDf = otherDf.select(otherDf.col("*"), posexplode_outer($"address1").as(Seq("p1", "a1")))
    otherDf = otherDf.withColumn("_p2", when($"p2".isNull, -1).otherwise($"p2")).drop("p2").withColumnRenamed("_p2", "p2")
    otherDf = otherDf.withColumn("valid1", $"p2"=== $"p1" || ($"p1" >= $"size_address2" && $"p2" <= 0) || ($"p1" < 0 && $"p2" <= 0 ))
    otherDf = otherDf.withColumn("valid2", $"p2"=== $"p1" || ($"p2" >= $"size_address1" && $"p1" <= 0) || ($"p2" < 0 && $"p1" <= 0 ))
    otherDf = otherDf.filter($"valid1" === true || $"valid2" === true)
    otherDf.show()
    otherDf.explain(true)
  }

  test("[Exploration] Incremental blowup") {
    val df = getDataFrame(pathToExampleDataFlatten)
    //first column
    var otherDf = df.withColumn("size_address1", size($"address1"))
    otherDf = otherDf.select(otherDf.col("*"), posexplode_outer($"address1").as(Seq("_p1", "a1")))
    otherDf = otherDf.withColumn("p1", when($"_p1".isNull, -1).otherwise($"_p1")).drop("_p1")
    //otherDf = otherDf.withColumn("_valid1", typedLit(Literal.TrueLiteral))
    //second column
    otherDf = otherDf.withColumn("size_address2", size($"address2"))
    otherDf = otherDf.select(otherDf.col("*"), posexplode_outer($"address2").as(Seq("_p2", "a2")))
    otherDf = otherDf.withColumn("p2", when($"_p2".isNull, -1).otherwise($"_p2")).drop("_p2")
    //compute valid fields, filter based on valid fields
    //                                                      zipping               address2 is larger than address1           address2 is empty (outer explode)
    otherDf = otherDf.withColumn("valid2", $"p2" === $"p1" || ($"p2" >= $"size_address1" && $"p1" <= 0 ) || ($"p1" <= 0 && $"p2" < 0))
    otherDf = otherDf.withColumn("valid1", $"p1" === $"p2" || ($"p1" >= $"size_address2" && $"p2" <= 0 ) || ($"p2" <= 0 && $"p1" < 0))
    otherDf = otherDf.filter($"valid2" === true || $"valid1" === true)
    //third column
    otherDf = otherDf.withColumn("size_address3", size($"address2"))
    otherDf = otherDf.select(otherDf.col("*"), posexplode_outer($"address2").as(Seq("_p3", "a3")))
    otherDf = otherDf.withColumn("p3", when($"_p3".isNull, -1).otherwise($"_p3")).drop("_p3")
    otherDf = otherDf.withColumn("valid1a", ($"p1" === $"p2" || ($"p1" >= $"size_address2" && $"p2" <= 0 ) || ($"p2" <= 0 && $"p1" < 0)) && ($"p1" === $"p3" || ($"p1" >= $"size_address3" && $"p3" <= 0 ) || ($"p3" <= 0 && $"p1" < 0)))
    otherDf = otherDf.withColumn("valid2a", ($"p2" === $"p1" || ($"p2" >= $"size_address1" && $"p1" <= 0 ) || ($"p1" <= 0 && $"p2" < 0)) && ($"p2" === $"p3" || ($"p2" >= $"size_address3" && $"p3" <= 0 ) || ($"p3" <= 0 && $"p2" < 0)))
    otherDf = otherDf.withColumn("valid3a", ($"p3" === $"p1" || ($"p3" >= $"size_address1" && $"p1" <= 0 ) || ($"p1" <= 0 && $"p3" < 0)) && ($"p3" === $"p1" || ($"p3" >= $"size_address3" && $"p2" <= 0 ) || ($"p2" <= 0 && $"p3" < 0)))
    otherDf = otherDf.drop("valid1").drop("valid2")
    otherDf = otherDf.filter($"valid1a" === true || $"valid2a" === true || "valid3a" === true)







    otherDf.show()
    otherDf.explain(true)

    //otherDf = otherDf.withColumn("size_address2", size($"address2"))
  }

  def createArrayFromColValue(colValue: Int): Seq[Int] = {
    (0 until colValue).toArray
  }

  def createArrayFromCol = (colValue: Int) => {
    (1 to colValue).toArray
  }



  //val temp = normalize_run.withColumn("dummy",createArrayFromColValueUDF($"REP"))



  test("[Exploration] Smart MultiFlatten") {
    val df = getDataFrame(pathToExampleDataFlatten)
    var otherDf = df.withColumn("size_address1", size($"address1"))
    otherDf = otherDf.withColumn("size_address2", size($"address2"))
    otherDf = otherDf.withColumn("max_size", greatest($"size_address1", $"size_address2", typedLit(Literal(1))))
    //val maxValUDF = udf(createArrayFromCol)
    ProvenanceContext.initializeUDF(otherDf)
    val maxValUDF = ProvenanceContext.getFlattenUDF
    otherDf = otherDf.withColumn("indexArray",maxValUDF($"max_size"))
    otherDf = otherDf.withColumn("idx", explode($"indexArray"))
    otherDf = otherDf.select(otherDf.col("*"),
      when($"idx" <= $"size_address1", element_at($"address1", $"idx")).otherwise(null).alias("address_alt1"),
      when($"idx" <= $"size_address2", element_at($"address2", $"idx")).otherwise(null).alias("address_alt2"))
    otherDf = otherDf.withColumn("valid1",
      when($"idx" <= $"size_address1" || ($"idx" === 1 && $"size_address1" === 0), true).otherwise(false))
    otherDf = otherDf.withColumn("valid2",
      when($"idx" <= $"size_address2" || ($"idx" === 1 && $"size_address2" === 0), true).otherwise(false))
    otherDf.show()
    otherDf.explain(true)
  }







  test("[Exploration] Get pos of nested array") {
    val df = getDataFrame(pathToExampleData)
    var otherDf = df.select(df.col("*"), posexplode_outer($"address2").as(Seq("p2", "a2")))
    otherDf.show()
    otherDf.explain(true)


  }

  def whyNotTupleUnnestedAddress(): Twig = {
    var twig = new Twig()
    val root = twig.createNode("root", 1, 1, "")
    val struct1 = twig.createNode("address", 1, 1, "")
    val element1 = twig.createNode("city", 1, 1, "LA")
    val element2 = twig.createNode("year", 1, 1, "")
    twig = twig.createEdge(root, struct1, false)
    twig = twig.createEdge(struct1, element1, false)
    twig = twig.createEdge(struct1, element2, false)
    twig.validate().get
  }

  test("[Exploration] Check multi-schema rewrite") {
    val df = getDataFrame(pathToExampleData)
    //val otherDf = df.select(df.col("*"), explode($"address2").alias("address"))
    val otherDf = df.select(explode($"address2").alias("address"))
    val res = WhyNotProvenance.rewriteWithAlternatives(otherDf, whyNotTupleUnnestedAddress())
    res.show()
    res.explain(true)
  }





//  test("[Unrestructure] Explode multiple collections simultaneously") {
//    val df = getDataFrame(pathToExampleData)
//    df.show(false)
//
//    val res = df.withColumn("addresses", explode(arrays_zip($"address1", $"address2"))).select($"name", $"vars.varA", $"vars.varB")
//    res.show(false)
//  }
}
