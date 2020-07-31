package de.uni_stuttgart.ipvs.provenance.evaluation

import java.io.{BufferedWriter, IOException, OutputStreamWriter}
import java.text.SimpleDateFormat

import org.apache.hadoop.fs.{FSDataOutputStream, FileSystem, Path}
import org.apache.spark.sql.SparkSession
import org.joda.time.{DateTime, DateTimeZone}

import scala.collection.mutable

object EvaluationResult {
  def apply(spark: SparkSession, testSuite: TestSuite) = new EvaluationResult(spark, testSuite)
}

class EvaluationResult(spark: SparkSession, testSuite: TestSuite) {

  lazy val resultBasePath = testSuite.getBasePath() + "measurements/"
  lazy val runPath = resultBasePath + "runs.csv"
  lazy val runsPath = resultBasePath + "aggregated_runs.csv"

  var runs = mutable.ListBuffer.empty[Long]


  def getAverageRuntime(): Long = {
    if (runs.size == 0) {
      return 0L
    }
    runs.sum / runs.size
  }

  def getMedianRuntime(): Long = {
    if (runs.size == 0) {
      return 0L
    }
    val (lower, upper) = runs.sortWith(_<_).splitAt(runs.size / 2)
    if (runs.size % 2 == 0) (lower.last + upper.head) / 2 else upper.head
  }

  def getRuntimeStandardDeviation: Double = {
    if (runs.size == 0) {
      return 0.0
    }
    val avg = getAverageRuntime().toDouble
    math.sqrt(runs.map(_.toDouble).map(run => math.pow(run - avg, 2)).sum / runs.size)
  }

  def reset(): Unit = {
    runs = mutable.ListBuffer.empty[Long]
  }

  def recordEntry(testScenario: TestScenario, iteration: Int, executionTime: Long) = {
    runs.append(executionTime)
    writeRunRow(testScenario, iteration, executionTime)
  }

  def getTimeString(): String = {
    val currentTimestamp = DateTime.now(DateTimeZone.UTC)
    currentTimestamp.formatted("yyyy-MM-dd'T'HH:mm:ss")
  }

  def getApplicationId: String = {
    spark.sparkContext.applicationId
  }

  def getNumExecutors: String = {
    spark.conf.getOption("spark.executor.instances").getOrElse((-1).toString)
  }

  def getNumExecutorCores: String = {
    spark.conf.getOption("spark.executor.cores").getOrElse((1).toString)
  }

  def getExecutorMemory: String = {
    spark.conf.getOption("spark.executor.memory").getOrElse((-1).toString)
  }

  def getSparkConfiguration: String = {
    val builder = scala.collection.mutable.StringBuilder.newBuilder
    builder.append(getNumExecutors)
    builder.append(";")
    builder.append(getExecutorMemory)
    builder.append(";")
    builder.append(getNumExecutorCores)
    builder.append(";")
    builder.toString()
  }

  def getSparkConfigurationHeader: String = {
    val builder = scala.collection.mutable.StringBuilder.newBuilder
    builder.append("Executors")
    builder.append(";")
    builder.append("ExecutorMemory")
    builder.append(";")
    builder.append("ExecutorCores")
    builder.append(";")
    builder.toString()
  }

  def getGenericCSVHeader: String = {
    val builder = scala.collection.mutable.StringBuilder.newBuilder
    builder.append("ApplicationId")
    builder.append(";")
    builder.append(getSparkConfigurationHeader)
    builder.append(testSuite.toCSVHeader())
    builder.toString()
  }

  def getGenericCSV: String = {
    val builder = scala.collection.mutable.StringBuilder.newBuilder
    builder.append(getApplicationId)
    builder.append(";")
    builder.append(getSparkConfiguration)
    builder.append(testSuite.toCSV())
    builder.toString()
  }

  def runToCSVHeader(testScenario: TestScenario, iteration: Int, executionTime: Long): String = {
    val builder = scala.collection.mutable.StringBuilder.newBuilder
    builder.append(getGenericCSVHeader)
    builder.append(testScenario.toCSVHeader(iteration, executionTime))
    builder.toString()
  }

  def runToCSV(testScenario: TestScenario, iteration: Int, executionTime: Long): String = {
    val builder = scala.collection.mutable.StringBuilder.newBuilder
    builder.append(getGenericCSV)
    builder.append(testScenario.toCSV(iteration, executionTime))
    builder.toString()
  }

  def runsToCSV(testScenario: TestScenario): String = {
    val builder = scala.collection.mutable.StringBuilder.newBuilder
    builder.append(getGenericCSV)
    builder.append(testScenario.toCSV())
    builder.append(getAverageRuntime())
    builder.append(";")
    builder.append(getMedianRuntime())
    builder.append(";")
    builder.append(getRuntimeStandardDeviation)
    builder.append(";")
    builder.toString()
  }

  def runsToCSVHeader(testScenario: TestScenario): String = {
    val builder = scala.collection.mutable.StringBuilder.newBuilder
    builder.append(getGenericCSVHeader)
    builder.append(testScenario.toCSVHeader())
    builder.append("Average")
    builder.append(";")
    builder.append("Median")
    builder.append(";")
    builder.append("STDDEV")
    builder.append(";")
    builder.toString()
  }


  def writeRunHeaderRow(): Unit = {
    writeRunRow(runToCSVHeader(DummyScenario(), 0, 0L))
  }

  def writeRunRow(testScenario: TestScenario, iteration: Int, executionTime: Long): Unit = {
    writeRunRow(runToCSV(testScenario,iteration,executionTime))
  }

  def writeRunRow(row: String): Unit = {
    writeRow(row, runPath)
  }

  def writeRunsHeaderRow(): Unit = {
    writeRunsRow(runsToCSVHeader(DummyScenario()))
  }

  def writeRunsRow(testScenario: TestScenario): Unit = {
    writeRunsRow(runsToCSV(testScenario))
  }

  def writeRunsRow(row: String): Unit = {
    writeRow(row, runsPath)
  }


  def writeRow(row: String, _path: String): Unit = {
    val hadoopConfiguration = spark.sparkContext.hadoopConfiguration
    val hdfs = org.apache.hadoop.fs.FileSystem.get(hadoopConfiguration)
    val path: Path = new Path(_path)
    val exists = hdfs.exists(path)
    var dataOutputStream: FSDataOutputStream = null
    if (exists) {
      dataOutputStream = hdfs.append(path)
    } else {
      dataOutputStream = hdfs.create(path)
    }
    val bw: BufferedWriter = new BufferedWriter(new OutputStreamWriter(dataOutputStream, "UTF-8"))
    bw.append(row).append("\n")
    bw.close
  }






  /*
  @throws[IOException]
  def writeResultToHDFS(hdfsPath: String, content: String, fs: FileSystem) {
    val path: Path = new Path(hdfsPath)
    if (fs.exists(path)) {
      fs.delete(path, true)
    }
    val dataOutputStream: FSDataOutputStream = fs.create(path)
    val bw: BufferedWriter = new BufferedWriter(new OutputStreamWriter(dataOutputStream, "UTF-8"))
    bw.write(content)
    bw.close
  }


  hfdsPath und content sind dabei klar, fs ist wie folgt zu setzen

  val spark = SparkSession.builder()
    .appName("interact-finding-k")
    .getOrCreate()
  val sc = spark.sparkContext
  val fs = FileSystem.get(sc.hadoopConfiguration)

   */

}

