package de.uni_stuttgart.ipvs.provenance.transformations

import com.github.mrpowers.spark.fast.tests.DataFrameComparer
import de.uni_stuttgart.ipvs.provenance.SharedSparkTestDataFrames
import de.uni_stuttgart.ipvs.provenance.nested_why_not.{Constants, WhyNotProvenance}
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





}