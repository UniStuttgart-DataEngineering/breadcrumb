package de.uni_stuttgart.ipvs.provenance.evaluation

import java.io.IOException
import java.net.URLClassLoader
import java.util.jar.Manifest

import de.uni_stuttgart.ipvs.provenance.evaluation.TestConfiguration.getClass
import org.slf4j.LoggerFactory

import scala.reflect.ManifestFactory

object TestConfiguration {

  private def toInt(s: String):Option[Int] = {
    try {
      Some(s.toInt)
    } catch {
      case e: NumberFormatException => None
    }
  }

  //0 -> reference
  //1 -> size
  //2 -> repetitions
  //3 -> warmup
  //4 -> testMask
  //5 -> dataPath
  //6 -> schemaAlternativeSize

  def apply(parameters: Array[String]) : TestConfiguration = {
    val testConfiguration = new TestConfiguration()
    testConfiguration.referenceScenario(toInt(parameters(0)).getOrElse(0))

//    testConfiguration._referenceScenario = parameters(0) match {
//      case "true" => true
//      case "false" => false
//      case _ => throw new MatchError("Reference query flag must be \"true\" or \"false\"")
//    }

    testConfiguration._dataSize = parameters(1) match {
      case  "100" => 100
      case  "200" => 200
      case  "300" => 300
      case  "400" => 400
      case  "500" => 500
      case _ => throw new MatchError("Data Size must be either of the values [100|200|300|400|500]")
    }
    val iterations = toInt(parameters(2))
    testConfiguration._iterations = iterations match {
      case Some(n) => n
      case _ => throw new MatchError("Repetitions must be between 1 and including 10")
    }

    testConfiguration._warmUp = parameters(3) match {
      case "true" => true
      case "false" => false
      case _ => throw new MatchError("Warmup flag must be \"true\" or \"false\"")
    }

    val testMask = toInt(parameters(4))
    testConfiguration._testMask = testMask match {
      case Some(n) => n
      case _ => throw new MatchError("TestMask should be Integer")
    }

    testConfiguration._pathToData = parameters(5)
    testConfiguration.schemaAlternativeSize(toInt(parameters(6)).getOrElse(1))
    testConfiguration.build()
  }

  def local(pathToData: String, numSchemaAlternatives: Int = 1) = {
    val testConfiguration = new TestConfiguration()
    testConfiguration._pathToData = pathToData
    testConfiguration._dataSize = -1
    testConfiguration._testMask = 127
    testConfiguration._local = true
    testConfiguration._schemaAlternativeSize = numSchemaAlternatives
    testConfiguration.build()
  }

}

class TestConfiguration {

  lazy val logger = LoggerFactory.getLogger(getClass)

  private var _referenceScenario: Int = 0
  private var _pathToData : String = "/"
  private var _iterations : Int = 1
  private var _warmUp: Boolean = false
  private var _dataSize : Int = 100
  private var _testMask = 0
  private var _schemaAlternativeSize: Int = 1

  private var _local = false

  private var _built = false

  private lazy val _commitId = getCommitId()




  def getZeros(): String = {
    _dataSize match {
      case 100 => "000000"
      case 200 => "00000"
      case 300 => "0000"
      case 400 => "000"
      case 500 => "00"
      case -1 => ""
      case _ => throw new RuntimeException("Something went wrong converting the input dataSize to zeros")
    }
  }


  def referenceScenario = _referenceScenario

  def referenceScenario(int: Int) : TestConfiguration = {
    if (!_built) _referenceScenario = int
    this
  }


  def pathToData = _pathToData

  def pathToData(path: String) : TestConfiguration = {
    if (!_built) _pathToData = path
    this
  }

  def iterations = _iterations

  def iterations(it: Int) : TestConfiguration = {
    if (!_built) _iterations = it
    this
  }

  def warmUp = _warmUp

  def warmUp(warmUp:Boolean) : TestConfiguration = {
    if (!_built) _warmUp = warmUp
    this
  }

  def testMask(testMask: Int): TestConfiguration = {
    if (!_built) _testMask = testMask
    this
  }

  def testMask = _testMask

  def dataSize = _dataSize

  def dataSize(dataSize:Int) : TestConfiguration = {
    if (!_built) _dataSize = dataSize
    this
  }

  def schemaAlternativeSize: Int = _schemaAlternativeSize

  def schemaAlternativeSize(saSize: Int) : TestConfiguration = {
    if (!_built) _schemaAlternativeSize = saSize
    this
  }

  def isLocal: Boolean = _local

  def isLocal(local: Boolean) : TestConfiguration = {
    if (!_built) _local = local
    this
  }

  def build() : TestConfiguration = {
    _built = true
    this
  }

  def commitId = _commitId


  private def getCommitId(): String = {
    if (isLocal) {
      return "local"
    }
    var clOpt: Option[URLClassLoader] = None
    try {
      clOpt = Some(this.getClass().getClassLoader().asInstanceOf[URLClassLoader])
    } catch  {
      case t: Throwable => logger.warn("Failed to get URLClassLoader", t)
    }
    val cl = clOpt.getOrElse(return "noCommit")
    val url = cl.findResource("META-INF/MANIFEST.MF")
    if (url == null) {
      logger.warn("Failed to get Manifest file")
      return "noManifest"
    }
    var manifestOpt : Option[Manifest] = None
    try {
      manifestOpt = Some(new Manifest(url.openStream()))
    }
    catch {
      case t: Throwable => logger.warn("Failed to read Manifest", t)
    }

    val manifest = manifestOpt.getOrElse(return "noReadManifest")
    if (manifest == null) {
      return "nullManifest"
    }
    val attributes = manifest.getAttributes("Versions")
    attributes.getValue("Implementation-SCM-Revision")
  }

  def toCSV() = {
    val builder = scala.collection.mutable.StringBuilder.newBuilder
    builder.append(referenceScenario)
    builder.append(";")
    builder.append(pathToData)
    builder.append(";")
    builder.append(iterations)
    builder.append(";")
    builder.append(warmUp)
    builder.append(";")
    builder.append(dataSize)
    builder.append(";")
    builder.append(testMask)
    builder.append(";")
    builder.append(commitId)
    builder.append(";")
    builder.append(schemaAlternativeSize  + 1)
    builder.append(";")
    builder.toString()
  }


  def toCSVHeader() = {
    val builder = scala.collection.mutable.StringBuilder.newBuilder
    builder.append("referenceScenario;")
    builder.append("pathToData;")
    builder.append("iterations;")
    builder.append("warmUp;")
    builder.append("dataSize;")
    builder.append("testMask;")
    builder.append("commitId;")
    builder.append("schemaAlternatives")
    builder.toString()
  }

}


