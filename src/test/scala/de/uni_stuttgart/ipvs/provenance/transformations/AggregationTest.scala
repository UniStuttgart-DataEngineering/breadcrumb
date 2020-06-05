package de.uni_stuttgart.ipvs.provenance.transformations

import com.github.mrpowers.spark.fast.tests.{ColumnComparer, DataFrameComparer}
import de.uni_stuttgart.ipvs.provenance.SharedSparkTestDataFrames
import de.uni_stuttgart.ipvs.provenance.nested_why_not.{Constants, WhyNotProvenance}
import de.uni_stuttgart.ipvs.provenance.why_not_question.Twig
import org.scalatest.FunSuite
import org.apache.spark.sql.functions.{collect_list, max, min, rand, struct, sum}
import org.apache.spark.sql.types.{ArrayType, StructType}

class AggregationTest extends FunSuite with SharedSparkTestDataFrames with DataFrameComparer with ColumnComparer {

  import spark.implicits._

  def sumWhyNotTuple(): Twig = {
    var twig = new Twig()
    val root = twig.createNode("root", 1, 1, "")
    val sum = twig.createNode("sum", 1, 1, "")
    val key = twig.createNode("key", 1, 1, "")
    twig = twig.createEdge(root, sum, false)
    twig = twig.createEdge(root, key, false)
    twig.validate().get
  }

  def basicWhyNotTuple(): Twig = {
    var twig = new Twig()
    val root = twig.createNode("root", 1, 1, "")
    val key = twig.createNode("key", 1, 1, "")
    twig = twig.createEdge(root, key, false)
    twig.validate().get
  }

  test("[Rewrite] Aggregation ") {
    val df = getDataFrame(pathToAggregationDoc0)
    val aggregatedDf = df.groupBy($"key").agg(sum($"value").alias("sum"))
    val res = WhyNotProvenance.rewrite(aggregatedDf, sumWhyNotTuple)
    res.explain()
    res.printSchema()
    res.show()
  }

  test("[Rewrite] Aggregation retains all original attributes") {
    val df = getDataFrame(pathToAggregationDoc0)
    val aggregatedDf = df.groupBy($"key").agg(sum($"value").alias("sum"))
    val res = WhyNotProvenance.rewrite(aggregatedDf, sumWhyNotTuple)
    assert(checkSchemaContainment(res, df))
  }



  test("[Rewrite] Aggregation aggregates previous provenance into a nested collection") {
    val df = getDataFrame(pathToAggregationDoc0)
    val rewrittenDf = WhyNotProvenance.rewrite(df, basicWhyNotTuple())
    val aggregatedDf = df.groupBy($"key").agg(sum($"value").alias("sum"))
    val res = WhyNotProvenance.rewrite(aggregatedDf, sumWhyNotTuple)

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
    val res = WhyNotProvenance.rewrite(aggregatedDf, sumWhyNotTuple)

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
  
}
