package de.uni_stuttgart.ipvs.provenance.transformations

import com.github.mrpowers.spark.fast.tests.DataFrameComparer
import de.uni_stuttgart.ipvs.provenance.SharedSparkTestDataFrames
import de.uni_stuttgart.ipvs.provenance.nested_why_not.WhyNotProvenance
import de.uni_stuttgart.ipvs.provenance.why_not_question.Twig
import org.scalatest.FunSuite

class FilterTest extends FunSuite with SharedSparkTestDataFrames with DataFrameComparer{

  import spark.implicits._



  test("Projection without provenance annotations does not add provenance annotations") {

    val df = singleInputColumnDataFrame()
    val otherDf = df.filter($"MyIntCol" > 2)
    val res = WhyNotProvenance.rewrite(otherDf, whyNotTuple())
    res.explain()
    res.show()
    //assertSmallDataFrameEquality(df, otherDf)

  }

}
