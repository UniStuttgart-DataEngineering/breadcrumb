package de.uni_stuttgart.ipvs.provenance.nested_why_not

import com.github.mrpowers.spark.fast.tests.DataFrameComparer
import de.uni_stuttgart.ipvs.provenance.SharedSparkTestDataFrames
import org.scalatest.FunSuite

import scala.collection.mutable

class MSRComputationTest extends FunSuite with SharedSparkTestDataFrames with DataFrameComparer {

  import spark.implicits._

  test("MSR call") {
    val df = singleInputColumnDataFrame()
    val otherDf = df.filter($"MyIntCol" > 10)
    val res = WhyNotProvenance.computeMSRs(otherDf, myIntColWhyNotQuestion())
    assert(res.count == 1)
  }

}
