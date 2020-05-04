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



  test("[Unrestructure] Attribute keeps name and structure"){
    val df = getDataFrame()
    val schemaMatch = getSchemaMatch(df, whyNotTupleProjection1())
    val schemaSubset = SchemaSubsetTree(schemaMatch, new Schema(df))
    val res = df.select($"flat_key")

    val rewrite = ProjectRewrite(res.queryExecution.analyzed.asInstanceOf[Project],schemaSubset, 1)
    val rewrittenSchemaMatch = rewrite.unrestructure()


    df.printSchema()
  }





}