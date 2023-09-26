package com.ucesys.sparkscope

import com.ucesys.sparklens.common.{AggregateMetrics, AppContext, ApplicationInfo}
import com.ucesys.sparklens.timespan.{ExecutorTimeSpan, HostTimeSpan, JobTimeSpan, StageTimeSpan}
import com.ucesys.sparkscope.SparkScopeAnalyzer._
import com.ucesys.sparkscope.data.DataFrame
import com.ucesys.sparkscope.io.{DriverExecutorMetrics, FileReader, FileReaderFactory, HadoopFileReader, PropertiesLoader, PropertiesLoaderFactory}
import com.ucesys.sparkscope.warning.MissingMetricsWarning
import org.apache.spark.SparkConf
import org.scalamock.scalatest.MockFactory
import org.scalatest.FunSuite

import java.io.FileNotFoundException
import java.util.Properties
import scala.collection.mutable

object TestHelpers extends FunSuite with MockFactory {
  final val appId = "app-20230101010819-test"
  final val StartTime: Long = 1695358645000L
  final val EndTime: Long = 1695358700000L
  val MetricsPropertiesPath = "path/to/metrics.properties"
  val csvMetricsPath = "/tmp/csv-metrics"
  val sparkConf = new SparkConf().set("spark.metrics.conf", MetricsPropertiesPath)

  val emptyFileCsv: String =
    """t,value
      |1695358645,0""".stripMargin

  val jvmHeapDriverCsv: String =
    """t,value
      |1695358645,283638704
      |1695358650,220341368
      |1695358655,316237016
      |1695358660,324206552
      |1695358665,281020200
      |1695358670,330815488
      |1695358675,266299072
      |1695358680,213096240
      |1695358685,263432152
      |1695358690,267797984
      |1695358695,277957632
      |1695358697,218993128
      |1695358700,303019416""".stripMargin

  val jvmHeapUsageDriverCsv: String =
    """t,value
      |1695358645,0.29708835490780305
      |1695358650,0.2307895700202284
      |1695358655,0.33123242180796525
      |1695358660,0.33957985925648254
      |1695358665,0.29433708882214016
      |1695358670,0.3465021794343767
      |1695358675,0.27892650790688495
      |1695358680,0.2231414560300187
      |1695358685,0.2758600946182343
      |1695358690,0.2804448454017367
      |1695358695,0.29112465705012697
      |1695358697,0.2293248100636884
      |1695358700,0.31738806634994077""".stripMargin

  val jvmHeapMaxDriverCsv: String =
    """t,value
      |1695358645,954728448
      |1695358650,954728448
      |1695358655,954728448
      |1695358660,954728448
      |1695358665,954728448
      |1695358670,954728448
      |1695358675,954728448
      |1695358680,954728448
      |1695358685,954728448
      |1695358690,954728448
      |1695358695,954728448
      |1695358697,954728448
      |1695358700,954728448""".stripMargin

  val jvmNonHeapDriverCsv: String =
    """t,value
      |1695358645,56840944
      |1695358650,61081904
      |1695358655,66992968
      |1695358660,69940328
      |1695358665,71093880
      |1695358670,72258224
      |1695358675,72592544
      |1695358680,72947528
      |1695358685,72327392
      |1695358690,72905864
      |1695358695,71930856
      |1695358697,70562064
      |1695358700,70981360""".stripMargin

  val jvmHeapExec1Csv: String =
    """t,value
      |1695358649,237607008
      |1695358654,95638712
      |1695358659,314070808
      |1695358664,369341672
      |1695358669,268593248
      |1695358674,345835736
      |1695358679,116886064
      |1695358684,336281224
      |1695358689,355456568
      |1695358694,113488816
      |1695358697,180952784""".stripMargin

  val jvmHeapUsageExec1Csv: String =
    """t,value
      |1695358649,0.2823698234558105
      |1695358654,0.11401022911071777
      |1695358659,0.37440157890319825
      |1695358664,0.4402895832061768
      |1695358669,0.32018810272216797
      |1695358674,0.4122585391998291
      |1695358679,0.13933904647827147
      |1695358684,0.4008784580230713
      |1695358689,0.42372746467590333
      |1695358694,0.13523258209228517
      |1695358697,0.21571252822875978""".stripMargin

  val jvmHeapMaxExec1Csv: String =
    """t,value
      |1695358649,838860800
      |1695358654,838860800
      |1695358659,838860800
      |1695358664,838860800
      |1695358669,838860800
      |1695358674,838860800
      |1695358679,838860800
      |1695358684,838860800
      |1695358689,838860800
      |1695358694,838860800
      |1695358697,838860800""".stripMargin

  val jvmNonHeapExec1Csv: String =
    """t,value
      |1695358649,42346768
      |1695358654,46358624
      |1695358659,49276240
      |1695358664,50268712
      |1695358669,50243840
      |1695358674,50339984
      |1695358679,50620984
      |1695358684,50314224
      |1695358689,50847152
      |1695358694,51045160
      |1695358697,51120384""".stripMargin

  val cpuTime1Csv: String =
    """t,count
      |1695358649,434428446
      |1695358654,2215933296
      |1695358659,4930102712
      |1695358664,7867871200
      |1695358669,11071045636
      |1695358674,13990111255
      |1695358679,17584148484
      |1695358684,20956416798
      |1695358689,23926534546
      |1695358694,27604708924
      |1695358697,29815418742""".stripMargin

  // ---------- EXEC 1 ----------
  val jvmHeapExec2Csv: String =
    """t,value
      |1695358649,195780664
      |1695358654,336201928
      |1695358659,266543080
      |1695358664,329492960
      |1695358669,217101296
      |1695358674,237873088
      |1695358679,307236648
      |1695358684,185639680
      |1695358689,166666808
      |1695358694,171814536
      |1695358697,275586688""".stripMargin

  val jvmHeapUsageExec2Csv: String =
    """t,value
      |1695358649,0.23338873863220214
      |1695358654,0.4007839298248291
      |1695358659,0.3177441120147705
      |1695358664,0.39278621673583985
      |1695358669,0.25880491256713867
      |1695358674,0.2835668182373047
      |1695358679,0.36625462532043457
      |1695358684,0.2212125301361084
      |1695358689,0.19867253303527832
      |1695358694,0.20476229667663573
      |1695358697,0.3285249328613281""".stripMargin

  val jvmHeapMaxExec2Csv: String =
    """t,value
      |1695358649,838860800
      |1695358654,838860800
      |1695358659,838860800
      |1695358664,838860800
      |1695358669,838860800
      |1695358674,838860800
      |1695358679,838860800
      |1695358684,838860800
      |1695358689,838860800
      |1695358694,838860800
      |1695358697,838860800""".stripMargin

  val jvmNonHeapExec2Csv: String =
    """t,value
      |1695358649,42811920
      |1695358654,46701168
      |1695358659,49800000
      |1695358664,50386096
      |1695358669,50998000
      |1695358674,51138536
      |1695358679,51364368
      |1695358684,50760920
      |1695358689,50842144
      |1695358694,50971768
      |1695358697,50638864""".stripMargin

  val cpuTime2Csv: String =
    """t,count
      |1695358649,438401162
      |1695358654,2182426098
      |1695358659,4922506921
      |1695358664,7901965708
      |1695358669,11104348527
      |1695358674,13980744341
      |1695358679,17616604346
      |1695358684,20937026849
      |1695358689,23771909531
      |1695358694,27322915542
      |1695358697,29625862807""".stripMargin

  // ---------- EXEC 2 ----------
  val jvmHeapExec3Csv: String =
    """t,value
      |1695358675,305435840
      |1695358680,262708600
      |1695358685,170042672
      |1695358690,144484944
      |1695358695,240152768
      |1695358697,169741776""".stripMargin

  val jvmHeapUsageExec3Csv: String =
    """t,value
      |1695358675,0.3641078948974609
      |1695358680,0.3131730556488037
      |1695358685,0.20223091125488282
      |1695358690,0.17223947525024413
      |1695358695,0.28628440856933596
      |1695358697,0.20234796524047852""".stripMargin

  val jvmHeapMaxExec3Csv: String =
    """t,value
      |1695358675,838860800
      |1695358680,838860800
      |1695358685,838860800
      |1695358690,838860800
      |1695358695,838860800
      |1695358697,838860800""".stripMargin

  val jvmNonHeapExec3Csv: String =
    """t,value
      |1695358675,47486624
      |1695358680,50236376
      |1695358685,49463504
      |1695358690,50636936
      |1695358695,51004320
      |1695358697,51158296""".stripMargin

  val cpuTime3Csv: String =
    """t,count
      |1695358675,2085769750
      |1695358680,5516790869
      |1695358685,8767561125
      |1695358690,11460149659
      |1695358695,15116281483
      |1695358697,16963354541""".stripMargin

  // ---------- EXEC 3 ----------
  val jvmHeapExec5Csv: String =
    """t,value
      |1695358691,199513160
      |1695358696,249545504
      |1695358697,255433896""".stripMargin

  val jvmHeapUsageExec5Csv: String =
    """t,value
      |1695358691,0.2378382205963135
      |1695358696,0.2974814224243164
      |1695358697,0.30450093269348144
      |""".stripMargin

  val jvmHeapMaxExec5Csv: String =
    """t,value
      |1695358691,838860800
      |1695358696,838860800
      |1695358697,838860800""".stripMargin

  val jvmNonHeapExec5Csv: String =
    """t,value
      |1695358691,47196664
      |1695358696,50223304
      |1695358697,49633008""".stripMargin

  val cpuTime5Csv: String =
    """t,count
      |1695358691,1877412152
      |1695358696,5451002249
      |1695358697,6129593128""".stripMargin

  val driverMetrics = Seq(
    DataFrame.fromCsv("driver-heap-used", jvmHeapDriverCsv, ",", Seq("t", JvmHeapUsed)),
    DataFrame.fromCsv("driver-heap-max", jvmHeapMaxDriverCsv, ",", Seq("t", JvmHeapMax)),
    DataFrame.fromCsv("driver-heap-usage", jvmHeapUsageDriverCsv, ",", Seq("t", JvmHeapUsage)),
    DataFrame.fromCsv("driver-non-heap", jvmNonHeapDriverCsv, ",", Seq("t", JvmNonHeapUsed))
  )

  val executorMetricsMap = Map(
    1 -> Seq(
      DataFrame.fromCsv("exec1-heap-used", jvmHeapExec1Csv, ",", Seq("t", JvmHeapUsed)),
      DataFrame.fromCsv("exec1-heap-max", jvmHeapMaxExec1Csv, ",", Seq("t", JvmHeapMax)),
      DataFrame.fromCsv("exec1-heap-usage", jvmHeapUsageExec1Csv, ",", Seq("t", JvmHeapUsage)),
      DataFrame.fromCsv("exec1-non-heap", jvmNonHeapExec1Csv, ",", Seq("t", JvmNonHeapUsed)),
      DataFrame.fromCsv("exec1-cpu-time", cpuTime1Csv, ",", Seq("t", CpuTime))
    ),
    2 -> Seq(
      DataFrame.fromCsv("exec2-heap-used", jvmHeapExec2Csv, ",", Seq("t", JvmHeapUsed)),
      DataFrame.fromCsv("exec2-heap-max", jvmHeapMaxExec2Csv, ",", Seq("t", JvmHeapMax)),
      DataFrame.fromCsv("exec2-heap-usage", jvmHeapUsageExec2Csv, ",", Seq("t", JvmHeapUsage)),
      DataFrame.fromCsv("exec2-non-heap", jvmNonHeapExec2Csv, ",", Seq("t", JvmNonHeapUsed)),
      DataFrame.fromCsv("exec2-cpu-time", cpuTime2Csv, ",", Seq("t", CpuTime))
    ),
    3 -> Seq(
      DataFrame.fromCsv("exec3-heap-used", jvmHeapExec3Csv, ",", Seq("t", JvmHeapUsed)),
      DataFrame.fromCsv("exec3-heap-max", jvmHeapMaxExec3Csv, ",", Seq("t", JvmHeapMax)),
      DataFrame.fromCsv("exec3-heap-usage", jvmHeapUsageExec3Csv, ",", Seq("t", JvmHeapUsage)),
      DataFrame.fromCsv("exec3-non-heap", jvmNonHeapExec3Csv, ",", Seq("t", JvmNonHeapUsed)),
      DataFrame.fromCsv("exec3-cpu-time", cpuTime3Csv, ",", Seq("t", CpuTime))
    ),
    5 -> Seq(
      DataFrame.fromCsv("exec5-heap-used", jvmHeapExec5Csv, ",", Seq("t", JvmHeapUsed)),
      DataFrame.fromCsv("exec5-heap-max", jvmHeapMaxExec5Csv, ",", Seq("t", JvmHeapMax)),
      DataFrame.fromCsv("exec5-heap-usage", jvmHeapUsageExec5Csv, ",", Seq("t", JvmHeapUsage)),
      DataFrame.fromCsv("exec5-non-heap", jvmNonHeapExec5Csv, ",", Seq("t", JvmNonHeapUsed)),
      DataFrame.fromCsv("exec5-cpu-time", cpuTime5Csv, ",", Seq("t", CpuTime))
    )
  )

  val DriverExecutorMetricsMock: DriverExecutorMetrics = DriverExecutorMetrics(
    driverMetrics = driverMetrics,
    executorMetricsMap = executorMetricsMap
  )

  val missingMetricsWarning = MissingMetricsWarning(Seq(1,2,3,4,5), Seq(1,2,3,5))
  def mockAppContext(): AppContext = {
    val executorMap: mutable.HashMap[String, ExecutorTimeSpan] = mutable.HashMap(
      "1" -> ExecutorTimeSpan("1", "0", 1, 1695358645000L, 1695358700000L),
      "2" -> ExecutorTimeSpan("2", "0", 1, 1695358645000L, 1695358700000L),
      "3" -> ExecutorTimeSpan("3", "0", 1, 1695358671000L, 1695358700000L),
      "5" -> ExecutorTimeSpan("5", "0", 1, 1695358687000L, 1695358700000L)
    )

    new AppContext(
      new ApplicationInfo(appId, StartTime, EndTime),
      new AggregateMetrics(),
      mutable.HashMap[String, HostTimeSpan](),
      executorMap,
      new mutable.HashMap[Long, JobTimeSpan],
      new mutable.HashMap[Long, Long],
      mutable.HashMap[Int, StageTimeSpan](),
      mutable.HashMap[Int, Long]())
  }

  def mockAppContextMissingExecutorMetrics() : AppContext = {
    mockAppContext.copy(
      executorMap = mutable.HashMap(
        "1" -> ExecutorTimeSpan("1", "0", 1, 1695358645000L, 1695358700000L),
        "2" -> ExecutorTimeSpan("2", "0", 1, 1695358645000L, 1695358700000L),
        "3" -> ExecutorTimeSpan("3", "0", 1, 1695358671000L, 1695358700000L),
        "5" -> ExecutorTimeSpan("5", "0", 1, 1695358687000L, 1695358700000L),
        "6" -> ExecutorTimeSpan("6", "0", 1, 1695358687000L, 1695358700000L)
      )
    )
  }

  def mockcorrectMetrics(csvReaderMock: HadoopFileReader): HadoopFileReader = {
    (csvReaderMock.read _).when(s"${csvMetricsPath}/${appId}.driver.jvm.heap.used.csv").returns(jvmHeapDriverCsv)
    (csvReaderMock.read _).when(s"${csvMetricsPath}/${appId}.driver.jvm.heap.usage.csv").returns(jvmHeapUsageDriverCsv)
    (csvReaderMock.read _).when(s"${csvMetricsPath}/${appId}.driver.jvm.heap.max.csv").returns(jvmHeapMaxDriverCsv)
    (csvReaderMock.read _).when(s"${csvMetricsPath}/${appId}.driver.jvm.non-heap.used.csv").returns(jvmNonHeapDriverCsv)

    // Starting from 1 on purpose

    (csvReaderMock.read _).when(s"${csvMetricsPath}/${appId}.1.jvm.heap.used.csv").returns(jvmHeapExec1Csv)
    (csvReaderMock.read _).when(s"${csvMetricsPath}/${appId}.1.jvm.heap.usage.csv").returns(jvmHeapUsageExec1Csv)
    (csvReaderMock.read _).when(s"${csvMetricsPath}/${appId}.1.jvm.heap.max.csv").returns(jvmHeapMaxExec1Csv)
    (csvReaderMock.read _).when(s"${csvMetricsPath}/${appId}.1.jvm.non-heap.used.csv").returns(jvmNonHeapExec1Csv)
    (csvReaderMock.read _).when(s"${csvMetricsPath}/${appId}.1.executor.cpuTime.csv").returns(cpuTime1Csv)

    (csvReaderMock.read _).when(s"${csvMetricsPath}/${appId}.2.jvm.heap.used.csv").returns(jvmHeapExec2Csv)
    (csvReaderMock.read _).when(s"${csvMetricsPath}/${appId}.2.jvm.heap.usage.csv").returns(jvmHeapUsageExec2Csv)
    (csvReaderMock.read _).when(s"${csvMetricsPath}/${appId}.2.jvm.heap.max.csv").returns(jvmHeapMaxExec2Csv)
    (csvReaderMock.read _).when(s"${csvMetricsPath}/${appId}.2.jvm.non-heap.used.csv").returns(jvmNonHeapExec2Csv)
    (csvReaderMock.read _).when(s"${csvMetricsPath}/${appId}.2.executor.cpuTime.csv").returns(cpuTime2Csv)
    
    (csvReaderMock.read _).when(s"${csvMetricsPath}/${appId}.3.jvm.heap.used.csv").returns(jvmHeapExec3Csv)
    (csvReaderMock.read _).when(s"${csvMetricsPath}/${appId}.3.jvm.heap.usage.csv").returns(jvmHeapUsageExec3Csv)
    (csvReaderMock.read _).when(s"${csvMetricsPath}/${appId}.3.jvm.heap.max.csv").returns(jvmHeapMaxExec3Csv)
    (csvReaderMock.read _).when(s"${csvMetricsPath}/${appId}.3.jvm.non-heap.used.csv").returns(jvmNonHeapExec3Csv)
    (csvReaderMock.read _).when(s"${csvMetricsPath}/${appId}.3.executor.cpuTime.csv").returns(cpuTime3Csv)

    // Missing one ide on purpose so that order of executor ids is not assumed
    (csvReaderMock.read _).when(s"${csvMetricsPath}/${appId}.5.jvm.heap.used.csv").returns(jvmHeapExec5Csv)
    (csvReaderMock.read _).when(s"${csvMetricsPath}/${appId}.5.jvm.heap.usage.csv").returns(jvmHeapUsageExec5Csv)
    (csvReaderMock.read _).when(s"${csvMetricsPath}/${appId}.5.jvm.heap.max.csv").returns(jvmHeapMaxExec5Csv)
    (csvReaderMock.read _).when(s"${csvMetricsPath}/${appId}.5.jvm.non-heap.used.csv").returns(jvmNonHeapExec5Csv)
    (csvReaderMock.read _).when(s"${csvMetricsPath}/${appId}.5.executor.cpuTime.csv").returns(cpuTime5Csv)

    (csvReaderMock.read _).when(s"${csvMetricsPath}/${appId}.6.jvm.heap.used.csv").throws(new FileNotFoundException)
    (csvReaderMock.read _).when(s"${csvMetricsPath}/${appId}.6.jvm.heap.usage.csv").throws(new FileNotFoundException)
    (csvReaderMock.read _).when(s"${csvMetricsPath}/${appId}.6.jvm.heap.max.csv").throws(new FileNotFoundException)
    (csvReaderMock.read _).when(s"${csvMetricsPath}/${appId}.6.jvm.non-heap.used.csv").throws(new FileNotFoundException)
    (csvReaderMock.read _).when(s"${csvMetricsPath}/${appId}.6.executor.cpuTime.csv").throws(new FileNotFoundException)

    csvReaderMock
  }

  def mockIncorrectDriverMetrics(csvReader: HadoopFileReader) = {
    (csvReader.read _).when(s"${csvMetricsPath}/${appId}.driver.jvm.heap.used.csv").returns(jvmHeapDriverCsv)
    (csvReader.read _).when(s"${csvMetricsPath}/${appId}.driver.jvm.heap.usage.csv").returns(jvmHeapUsageDriverCsv)
    (csvReader.read _).when(s"${csvMetricsPath}/${appId}.driver.jvm.heap.max.csv").returns(jvmHeapMaxDriverCsv)
    (csvReader.read _).when(s"${csvMetricsPath}/${appId}.driver.jvm.non-heap.used.csv").returns(emptyFileCsv)
    csvReader
  }

  def getPropertiesLoaderFactoryMock(loader: PropertiesLoader): PropertiesLoaderFactory = {
    val propertiesLoaderFactoryMock = stub[PropertiesLoaderFactory]
    (propertiesLoaderFactoryMock.getPropertiesLoader _).when(MetricsPropertiesPath).returns(loader)
    propertiesLoaderFactoryMock
  }

  def getPropertiesLoaderFactoryMock(): PropertiesLoaderFactory = {
    val propertiesLoaderFactoryMock = stub[PropertiesLoaderFactory]
    (propertiesLoaderFactoryMock.getPropertiesLoader _).when(MetricsPropertiesPath).returns(getPropertiesLoaderMock)
    propertiesLoaderFactoryMock
  }

  def getFileReaderFactoryMock(reader: FileReader): FileReaderFactory = {
    val fileReaderFactoryMock = stub[FileReaderFactory]
    (fileReaderFactoryMock.getFileReader _).when(*).returns(reader)
    fileReaderFactoryMock
  }

  def getPropertiesLoaderMock: PropertiesLoader = {
    val propertiesLoaderMock = stub[PropertiesLoader]
    val properties = new Properties()
    properties.setProperty("*.sink.csv.directory", csvMetricsPath)
    (propertiesLoaderMock.load _).when().returns(properties)
    propertiesLoaderMock
  }
}
