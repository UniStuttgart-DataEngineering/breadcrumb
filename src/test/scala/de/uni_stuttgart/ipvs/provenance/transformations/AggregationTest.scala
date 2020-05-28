package de.uni_stuttgart.ipvs.provenance.transformations

import com.github.mrpowers.spark.fast.tests.{ColumnComparer, DataFrameComparer}
import de.uni_stuttgart.ipvs.provenance.SharedSparkTestDataFrames
import de.uni_stuttgart.ipvs.provenance.nested_why_not.{Constants, WhyNotProvenance}
import de.uni_stuttgart.ipvs.provenance.why_not_question.Twig
import org.apache.spark.sql.catalyst.expressions.{LessThanOrEqual, Literal, Rand}
import org.scalatest.FunSuite
import org.apache.spark.sql.functions.{collect_list, struct, sum, rand, min, max}

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

  test("[Rewrite] Aggregation ") {
    val df = getDataFrame(pathToAggregationDoc0)
    val otherDf = df.groupBy($"key").agg(sum($"value").alias("sum"))
    //otherDf.explain(true)
    //otherDf.show()
    val res = WhyNotProvenance.rewrite(otherDf, sumWhyNotTuple)
    res.explain()
    res.printSchema()
    res.show()
    //assert(res.schema.filter(field => field.name.contains(Constants.SURVIVED_FIELD)).size == 1)
  }

  test("[Rewrite] Aggregation retains all original attributes") {
    val df = getDataFrame(pathToAggregationDoc0)
    val otherDf = df.groupBy($"key").agg(sum($"value").alias("sum"))
    val res = WhyNotProvenance.rewrite(otherDf, sumWhyNotTuple)
    assert(checkSchemaContainment(res, df))
  }

  test("[Rewrite] Aggregation aggregates previous provenance into a nested collection") {
    val df = getDataFrame(pathToAggregationDoc0)
    val otherDf = df.groupBy($"key").agg(sum($"value").alias("sum"))








  }

  test("[Rewrite] Aggregation adds top-level compatible column") {

  }


  test("[Rewrite] Aggregation nests provenance context") {


  }




  test("[Rewrite] Tuple ") {
    val df = getDataFrame(pathToAggregationDoc0)
    val otherDf = df.select($"key", struct($"key", $"value").alias("test"))
    otherDf.explain()

  }

  test("[Rewrite] ListCollection ") {
    val df = getDataFrame(pathToAggregationDoc0)
    val otherDf = df.groupBy($"key").agg(collect_list($"value").alias("list"))
    otherDf.explain(true)

    //otherDf.show()
    //val res = WhyNotProvenance.rewrite(otherDf, sumWhyNotTuple)
    //res.explain()
    //res.show()
    //assert(res.schema.filter(field => field.name.contains(Constants.SURVIVED_FIELD)).size == 1)
  }

  test("[Rewrite] Aggregate boolean Values ") {
    val df = getDataFrame(pathToAggregationDoc0)
    var testDf = df.withColumn("boolCol", rand(42) < 0.5)
    testDf = testDf.groupBy($"key").agg(collect_list($"boolCol").alias("list"), max($"boolCol"), min($"boolCol"))
  }







}
