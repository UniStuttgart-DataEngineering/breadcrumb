package de.uni_stuttgart.ipvs.provenance.transformations

import com.github.mrpowers.spark.fast.tests.{ColumnComparer, DataFrameComparer}
import de.uni_stuttgart.ipvs.provenance.SharedSparkTestDataFrames
import de.uni_stuttgart.ipvs.provenance.nested_why_not.{Constants, WhyNotProvenance}
import de.uni_stuttgart.ipvs.provenance.schema_alternatives.SchemaSubsetTree
import de.uni_stuttgart.ipvs.provenance.why_not_question.{Schema, Twig}
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.catalyst.plans.logical.{Aggregate, Generate}
import org.scalatest.FunSuite
import org.apache.spark.sql.functions.{collect_list, max, min, rand, struct, sum}
import org.apache.spark.sql.types.{ArrayType, StructType}

class AggregationTest extends FunSuite with SharedSparkTestDataFrames with DataFrameComparer with ColumnComparer {

  import spark.implicits._

  def getInputAndOutputWhyNotTuple(outputDataFrame: DataFrame, outputWhyNotTuple: Twig): (SchemaSubsetTree, SchemaSubsetTree) = {
    val schemaMatch = getSchemaMatch(outputDataFrame, outputWhyNotTuple)
    val schemaSubset = SchemaSubsetTree(schemaMatch, new Schema(outputDataFrame))

    val rewrite = AggregateRewrite(outputDataFrame.queryExecution.analyzed.asInstanceOf[Aggregate], -1)
    (rewrite.undoSchemaModifications(schemaSubset), schemaSubset)
  }

  def keySumWhyNotTuple(): Twig = {
    var twig = new Twig()
    val root = twig.createNode("root", 1, 1, "")
    val sum = twig.createNode("sum", 1, 1, "")
    val key = twig.createNode("key", 1, 1, "")
    twig = twig.createEdge(root, sum, false)
    twig = twig.createEdge(root, key, false)
    twig.validate().get
  }

  def keySumWhyNotTupleWithConditionOnSum(): Twig = {
    var twig = new Twig()
    val root = twig.createNode("root", 1, 1, "")
    val sum = twig.createNode("sum", 1, 1, "1000101")
    val key = twig.createNode("key", 1, 1, "")
    twig = twig.createEdge(root, sum, false)
    twig = twig.createEdge(root, key, false)
    twig.validate().get
  }

  def keySumWhyNotTupleWithConditionOnKey(): Twig = {
    var twig = new Twig()
    val root = twig.createNode("root", 1, 1, "")
    val sum = twig.createNode("sum", 1, 1, "")
    val key = twig.createNode("key", 1, 1, "1")
    twig = twig.createEdge(root, sum, false)
    twig = twig.createEdge(root, key, false)
    twig.validate().get
  }

  def sumWhyNotTuple(): Twig = {
    var twig = new Twig()
    val root = twig.createNode("root", 1, 1, "")
    val sum = twig.createNode("sum", 1, 1, "")
    twig = twig.createEdge(root, sum, false)
    twig.validate().get
  }


  def keyWhyNotTuple(): Twig = {
    var twig = new Twig()
    val root = twig.createNode("root", 1, 1, "")
    val key = twig.createNode("key", 1, 1, "")
    twig = twig.createEdge(root, key, false)
    twig.validate().get
  }

  test("[Unrestructure] Retain grouping attribute") {
    val df = getDataFrame(pathToAggregationDoc0)
    val aggregatedDf = df.groupBy($"key").agg(sum($"value").alias("sum"))
    val (rewrittenSchemaSubset, schemaSubset) = getInputAndOutputWhyNotTuple(aggregatedDf, keyWhyNotTuple())
    assert(schemaSubset.rootNode.name == rewrittenSchemaSubset.rootNode.name)
    assert(rewrittenSchemaSubset.rootNode.children.size == 1)
    assert(rewrittenSchemaSubset.rootNode.children.head.name == "key")
  }

  test("[Unrestructure] Retain condition in grouping attribute") {
    val df = getDataFrame(pathToAggregationDoc0)
    val aggregatedDf = df.groupBy($"key").agg(sum($"value").alias("sum"))
    val (rewrittenSchemaSubset, schemaSubset) = getInputAndOutputWhyNotTuple(aggregatedDf, keySumWhyNotTupleWithConditionOnKey())
    assert(schemaSubset.rootNode.name == rewrittenSchemaSubset.rootNode.name)
    assert(rewrittenSchemaSubset.rootNode.children.size == 2)
    assert(rewrittenSchemaSubset.rootNode.children.filter(child => child.name == "key")
      .headOption.getOrElse(fail("value not part of input schema subset")).constraint.constraintString == "1")
  }

  def nestedKeyTwig(): Twig = {
    var twig = new Twig()
    val root = twig.createNode("root", 1, 1, "")
    val sum = twig.createNode("some_val", 1, 1, "")
    val key = twig.createNode("nested_key", 1, 1, "")
    twig = twig.createEdge(root, sum, false)
    twig = twig.createEdge(root, key, false)
    twig.validate().get
  }

  test("[Unrestructure] Retain nested grouping attribute") {
    val df = getDataFrame(pathToNestedData1)
    val aggregatedDf = df.groupBy($"nested_obj.nested_obj.nested_key").agg(max($"flat_key").alias("some_val"))
    val (rewrittenSchemaSubset, schemaSubset) = getInputAndOutputWhyNotTuple(aggregatedDf, nestedKeyTwig())
    assert(schemaSubset.rootNode.name == rewrittenSchemaSubset.rootNode.name)
    assert(rewrittenSchemaSubset.rootNode.children.size == 2)
    val nested_obj1 = rewrittenSchemaSubset.rootNode.children.filter(child => child.name == "nested_obj")
      .headOption.getOrElse(fail("nested_obj not part of input schema subset"))
    assert(nested_obj1.children.size == 1)
    val nested_obj2 = nested_obj1.children.filter(child => child.name == "nested_obj")
      .headOption.getOrElse(fail("nested_obj not part of input schema subset"))
    assert(nested_obj2.children.size == 1)
    nested_obj2.children.filter(child => child.name == "nested_key")
      .headOption.getOrElse(fail("nested_key not part of input schema subset"))
  }

  test("[Unrestructure] Unrestructure aggregation attribute") {
    val df = getDataFrame(pathToAggregationDoc0)
    val aggregatedDf = df.groupBy($"key").agg(sum($"value").alias("sum"))
    val (rewrittenSchemaSubset, schemaSubset) = getInputAndOutputWhyNotTuple(aggregatedDf, sumWhyNotTuple())
    assert(schemaSubset.rootNode.name == rewrittenSchemaSubset.rootNode.name)
    assert(rewrittenSchemaSubset.rootNode.children.size == 1)
    assert(rewrittenSchemaSubset.rootNode.children.head.name == "value")

  }

  def nestedValTwig(): Twig = {
    var twig = new Twig()
    val root = twig.createNode("root", 1, 1, "")
    val sum = twig.createNode("some_val", 1, 1, "")
    val key = twig.createNode("flat_key", 1, 1, "")
    twig = twig.createEdge(root, sum, false)
    twig = twig.createEdge(root, key, false)
    twig.validate().get
  }

  test("[Unrestructure] Unrestructure nested aggregation attribute") {
    val df = getDataFrame(pathToNestedData1)
    val aggregatedDf = df.groupBy($"flat_key").agg(max($"nested_obj.nested_obj.nested_key").alias("some_val"))
    val (rewrittenSchemaSubset, _) = getInputAndOutputWhyNotTuple(aggregatedDf, nestedValTwig())
    assert(rewrittenSchemaSubset.rootNode.children.size == 2)
    val nested_obj1 = rewrittenSchemaSubset.rootNode.children.filter(child => child.name == "nested_obj")
      .headOption.getOrElse(fail("nested_obj not part of input schema subset"))
    assert(nested_obj1.children.size == 1)
    val nested_obj2 = nested_obj1.children.filter(child => child.name == "nested_obj")
      .headOption.getOrElse(fail("nested_obj not part of input schema subset"))
    assert(nested_obj2.children.size == 1)
    nested_obj2.children.filter(child => child.name == "nested_key")
      .headOption.getOrElse(fail("nested_key not part of input schema subset"))
  }



  test("[Unrestructure] Unrestructure aggregated value, remove constraint") {
    val df = getDataFrame(pathToAggregationDoc0)
    val aggregatedDf = df.groupBy($"key").agg(sum($"value").alias("sum"))
    val (rewrittenSchemaSubset, schemaSubset) = getInputAndOutputWhyNotTuple(aggregatedDf, sumWhyNotTuple())
    assert(rewrittenSchemaSubset.rootNode.children.filter(child => child.name == "value")
      .headOption.getOrElse(fail("value not part of input schema subset")).constraint.constraintString.isEmpty)
  }

  def nestedColTwig(): Twig = {
    var twig = new Twig()
    val root = twig.createNode("root", 1, 1, "")
    val list = twig.createNode("list", 1, 100, "")
    val element = twig.createNode("element", 1, 1, "1")
    twig = twig.createEdge(root, list, false)
    twig = twig.createEdge(list, element, false)
    twig.validate().get
  }

  test("[Unrestructure] Unrestructure aggregated collection element") {
    val df = getDataFrame(pathToAggregationDoc0)
    val aggregatedDf = df.groupBy($"key").agg(collect_list($"value").alias("list"))
    val (rewrittenSchemaSubset, _) = getInputAndOutputWhyNotTuple(aggregatedDf, nestedColTwig())
    assert(rewrittenSchemaSubset.rootNode.children.size == 1)
    assert(rewrittenSchemaSubset.rootNode.children.head.name == "value")
  }

  test("[Unrestructure] Unrestructure aggregated collection element removes cardinality constraint") {
    val df = getDataFrame(pathToAggregationDoc0)
    val aggregatedDf = df.groupBy($"key").agg(collect_list($"value").alias("list"))
    val (rewrittenSchemaSubset, _) = getInputAndOutputWhyNotTuple(aggregatedDf, nestedColTwig())
    assert(rewrittenSchemaSubset.rootNode.constraint.max == -1)
    assert(rewrittenSchemaSubset.rootNode.constraint.min == 1)
    assert(rewrittenSchemaSubset.rootNode.children.size == 1)
    assert(rewrittenSchemaSubset.rootNode.children.head.name == "value")
    assert(rewrittenSchemaSubset.rootNode.children.head.constraint.min == 1)
    assert(rewrittenSchemaSubset.rootNode.children.head.constraint.max == -1)
  }

  test("[Unrestructure] Unrestructure aggregated collection element retains value constraint") {
    val df = getDataFrame(pathToAggregationDoc0)
    val aggregatedDf = df.groupBy($"key").agg(collect_list($"value").alias("list"))
    val (rewrittenSchemaSubset, _) = getInputAndOutputWhyNotTuple(aggregatedDf, nestedColTwig())

    assert(rewrittenSchemaSubset.rootNode.children.size == 1)
    assert(rewrittenSchemaSubset.rootNode.children.head.name == "value")
    assert(rewrittenSchemaSubset.rootNode.children.head.constraint.constraintString == "1")
    assert(rewrittenSchemaSubset.rootNode.children.head.constraint.max == -1)
  }

  test("[Rewrite] Aggregation") {
    val df = getDataFrame(pathToAggregationDoc0)
    val aggregatedDf = df.groupBy($"key").agg(sum($"value").alias("sum"))
    val res = WhyNotProvenance.rewrite(aggregatedDf, keySumWhyNotTuple)
    res.explain()
    res.printSchema()
    res.show()
  }

  test("[Rewrite] Aggregation retains all original attributes") {
    val df = getDataFrame(pathToAggregationDoc0)
    val aggregatedDf = df.groupBy($"key").agg(sum($"value").alias("sum"))
    val res = WhyNotProvenance.rewrite(aggregatedDf, keySumWhyNotTuple)
    assert(checkSchemaContainment(res, df))
  }



  test("[Rewrite] Aggregation aggregates previous provenance into a nested collection") {
    val df = getDataFrame(pathToAggregationDoc0)
    val rewrittenDf = WhyNotProvenance.rewrite(df, keyWhyNotTuple())
    val aggregatedDf = df.groupBy($"key").agg(sum($"value").alias("sum"))
    val res = WhyNotProvenance.rewrite(aggregatedDf, keySumWhyNotTuple)

    val nestedAttribute = res.schema.filter(p => p.name.contains(Constants.PROVENANCE_COLLECTION))(0)
    nestedAttribute.dataType match {
      case a : ArrayType => {
        a.elementType match {
          case struct: StructType => {
            assert(struct.fields.size == rewrittenDf.columns.filter(name =>  Constants.columnNameContainsProvenanceConstant(name)).size)
            struct.fields.foreach(p => assert(Constants.columnNameContainsProvenanceConstant(p.name)))
          }
          case _ => fail("Aggregated provenance collection does not contain a tuple")
        }
      }
      case _ => fail("Aggregate rewrite does not contain a nested collection")
    }
  }

  test("[Rewrite] Aggregation adds top-level compatible column") {
    val df = getDataFrame(pathToAggregationDoc0)
    val aggregatedDf = df.groupBy($"key").agg(sum($"value").alias("sum"))
    val res = WhyNotProvenance.rewrite(aggregatedDf, keySumWhyNotTuple)

    assert(res.columns.filter(name => name.contains(Constants.COMPATIBLE_FIELD)).size == 1)
  }

  test("[Exploration] Tuple ") {
    val df = getDataFrame(pathToAggregationDoc0)
    val otherDf = df.select($"key", struct($"key", $"value").alias("test"))
    otherDf.explain()

  }

  test("[Exploration] ListCollection ") {
    val df = getDataFrame(pathToAggregationDoc0)
    val otherDf = df.groupBy($"key").agg(collect_list($"value").alias("list"))
    otherDf.explain(true)
  }

  test("[Exploration] Aggregate boolean Values ") {
    val df = getDataFrame(pathToAggregationDoc0)
    var testDf = df.withColumn("boolCol", rand(42) < 0.5)
    testDf = testDf.groupBy($"key").agg(collect_list($"boolCol").alias("list"), max($"boolCol"), min($"boolCol"))
  }


  test("[Unrestructure] Aggregate with a single function 1") {
    val df = getDataFrame(pathToAggregationDoc0)
    val res = df.groupBy($"key").agg(sum($"value").alias("sum"))

    val plan = res.queryExecution.analyzed
    val schemaSubset = getSchemaSubsetTree(res, sumWhyNotTuple())
    val rewrittenSchemaSubset = getInputAndOutputWhyNotTupleFlex(plan, schemaSubset, "")

    assert(schemaSubset.rootNode.name === rewrittenSchemaSubset.rootNode.name)
    val key = rewrittenSchemaSubset.rootNode.children.find(node => node.name == "key").getOrElse(fail("key not where it is supposed to be"))
    val value = rewrittenSchemaSubset.rootNode.children.find(node => node.name == "value").getOrElse(fail("value not where it is supposed to be"))

    assert(key.name == "key")
    assert(value.name == "value")
  }


  def collectListWhyNotTuple(): Twig = {
    var twig = new Twig()
    val root = twig.createNode("root", 1, 1, "")
    val list = twig.createNode("list", 1, 1, "")
    val key = twig.createNode("key", 1, 1, "")
    twig = twig.createEdge(root, list, false)
    twig = twig.createEdge(root, key, false)
    twig.validate().get
  }


  test("[Unrestructure] Aggregate with a single function 2") {
    val df = getDataFrame(pathToAggregationDoc0)
    val res = df.groupBy($"key").agg(collect_list($"value").alias("list"))

    val plan = res.queryExecution.analyzed
    val schemaSubset = getSchemaSubsetTree(res, collectListWhyNotTuple())
    val rewrittenSchemaSubset = getInputAndOutputWhyNotTupleFlex(plan, schemaSubset, "")

    assert(schemaSubset.rootNode.name === rewrittenSchemaSubset.rootNode.name)
    val key = rewrittenSchemaSubset.rootNode.children.find(node => node.name == "key").getOrElse(fail("key not where it is supposed to be"))
    val value = rewrittenSchemaSubset.rootNode.children.find(node => node.name == "value").getOrElse(fail("value not where it is supposed to be"))

    assert(key.name == "key")
    assert(value.name == "value")
  }


  def doubleAggWhyNotTuple(): Twig = {
    var twig = new Twig()
    val root = twig.createNode("root", 1, 1, "")
    val key = twig.createNode("key", 1, 1, "")
    val maxSum = twig.createNode("maxSum", 1, 1, "")
    twig = twig.createEdge(root, key, false)
    twig = twig.createEdge(root, maxSum, false)
    twig.validate().get
  }


  test("[Unrestructure] Aggregate over an aggregate") {
    val df = getDataFrame(pathToAggregationDoc0)
    var res = df.groupBy($"key").agg(sum($"value").alias("sum"))
    res = res.groupBy($"key").agg(max($"sum").alias("maxSum"))

    val plan = res.queryExecution.analyzed
    val childPlan = plan.children.head
    val schemaSubset = getSchemaSubsetTree(res, doubleAggWhyNotTuple())

    val rewrittenSchemaSubset1 = getInputAndOutputWhyNotTupleFlex(plan, schemaSubset, "")
    val rewrittenSchemaSubset2 = getInputAndOutputWhyNotTupleFlex(childPlan, rewrittenSchemaSubset1, "")

    assert(schemaSubset.rootNode.name === rewrittenSchemaSubset1.rootNode.name)
    assert(schemaSubset.rootNode.name === rewrittenSchemaSubset2.rootNode.name)

    var key = rewrittenSchemaSubset1.rootNode.children.find(node => node.name == "key").getOrElse(fail("key not where it is supposed to be"))
    var value = rewrittenSchemaSubset1.rootNode.children.find(node => node.name == "sum").getOrElse(fail("sum not where it is supposed to be"))

    assert(key.name == "key")
    assert(value.name == "sum")

    key = rewrittenSchemaSubset2.rootNode.children.find(node => node.name == "key").getOrElse(fail("key not where it is supposed to be"))
    value = rewrittenSchemaSubset2.rootNode.children.find(node => node.name == "value").getOrElse(fail("value not where it is supposed to be"))

    assert(key.name == "key")
    assert(value.name == "value")
  }


}
