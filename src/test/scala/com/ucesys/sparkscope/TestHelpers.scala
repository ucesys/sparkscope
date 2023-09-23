package com.ucesys.sparkscope

import com.qubole.sparklens.common.{AggregateMetrics, AppContext, ApplicationInfo}
import com.qubole.sparklens.timespan.{ExecutorTimeSpan, HostTimeSpan, JobTimeSpan, StageTimeSpan}
import com.ucesys.sparkscope.io.{CsvHadoopReader, PropertiesLoader}
import org.apache.spark.SparkConf
import org.scalamock.scalatest.MockFactory
import org.scalatest.funsuite.AnyFunSuite

import java.util.Properties
import scala.collection.mutable

object TestHelpers extends AnyFunSuite with MockFactory {
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

  val jvmHeapExec0Csv: String =
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

  val jvmHeapUsageExec0Csv: String =
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

  val jvmHeapMaxExec0Csv: String =
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

  val jvmNonHeapExec0Csv: String =
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

  val cpuTime0Csv: String =
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
  val jvmHeapExec1Csv: String =
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

  val jvmHeapUsageExec1Csv: String =
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

  val cpuTime1Csv: String =
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
  val jvmHeapExec2Csv: String =
    """t,value
      |1695358675,305435840
      |1695358680,262708600
      |1695358685,170042672
      |1695358690,144484944
      |1695358695,240152768
      |1695358697,169741776""".stripMargin

  val jvmHeapUsageExec2Csv: String =
    """t,value
      |1695358675,0.3641078948974609
      |1695358680,0.3131730556488037
      |1695358685,0.20223091125488282
      |1695358690,0.17223947525024413
      |1695358695,0.28628440856933596
      |1695358697,0.20234796524047852""".stripMargin

  val jvmHeapMaxExec2Csv: String =
    """t,value
      |1695358675,838860800
      |1695358680,838860800
      |1695358685,838860800
      |1695358690,838860800
      |1695358695,838860800
      |1695358697,838860800""".stripMargin

  val jvmNonHeapExec2Csv: String =
    """t,value
      |1695358675,47486624
      |1695358680,50236376
      |1695358685,49463504
      |1695358690,50636936
      |1695358695,51004320
      |1695358697,51158296""".stripMargin

  val cpuTime2Csv: String =
    """t,count
      |1695358675,2085769750
      |1695358680,5516790869
      |1695358685,8767561125
      |1695358690,11460149659
      |1695358695,15116281483
      |1695358697,16963354541""".stripMargin

  // ---------- EXEC 3 ----------
  val jvmHeapExec3Csv: String =
    """t,value
      |1695358691,199513160
      |1695358696,249545504
      |1695358697,255433896""".stripMargin

  val jvmHeapUsageExec3Csv: String =
    """t,value
      |1695358691,0.2378382205963135
      |1695358696,0.2974814224243164
      |1695358697,0.30450093269348144
      |""".stripMargin

  val jvmHeapMaxExec3Csv: String =
    """t,value
      |1695358691,838860800
      |1695358696,838860800
      |1695358697,838860800""".stripMargin

  val jvmNonHeapExec3Csv: String =
    """t,value
      |1695358691,47196664
      |1695358696,50223304
      |1695358697,49633008""".stripMargin

  val cpuTime3Csv: String =
    """t,count
      |1695358691,1877412152
      |1695358696,5451002249
      |1695358697,6129593128""".stripMargin

  def createDummyAppContext(): AppContext = {

    val jobMap = new mutable.HashMap[Long, JobTimeSpan]
    for (i <- 1 to 4) {
      jobMap(i) = new JobTimeSpan(i)
    }

    val jobSQLExecIDMap = new mutable.HashMap[Long, Long]
    val r = scala.util.Random
    val sqlExecutionId = r.nextInt(10000)

    // Let, Job 1, 2 and 3 have same sqlExecutionId
    jobSQLExecIDMap(1) = sqlExecutionId
    jobSQLExecIDMap(2) = sqlExecutionId
    jobSQLExecIDMap(3) = sqlExecutionId
    jobSQLExecIDMap(4) = r.nextInt(10000)

    // Let, Job 2 and 3 are not running in parallel, even though they have same sqlExecutionId
    val baseTime = 1L
    jobMap(1).setStartTime(baseTime)
    jobMap(1).setEndTime(baseTime + 5L)

    jobMap(2).setStartTime(baseTime + 3L)
    jobMap(2).setEndTime(baseTime + 6L)

    jobMap(3).setStartTime(baseTime + 7L)
    jobMap(3).setEndTime(baseTime + 9L)

    jobMap(4).setStartTime(baseTime + 10L)
    jobMap(4).setEndTime(baseTime + 12L)

    val executorMap: mutable.HashMap[String, ExecutorTimeSpan] = mutable.HashMap()
    val executor0Timespan = new ExecutorTimeSpan("0", "0", 1)
    val executor1Timespan = new ExecutorTimeSpan("1", "0", 1)
    val executor2Timespan = new ExecutorTimeSpan("2", "0", 1)
    val executor3Timespan = new ExecutorTimeSpan("3", "0", 1)
    executor0Timespan.setStartTime(1695358645000L)
    executor0Timespan.setEndTime(1695358700000L)
    executor1Timespan.setStartTime(1695358645000L)
    executor1Timespan.setEndTime(1695358700000L)
    executor2Timespan.setStartTime(1695358671000L)
    executor2Timespan.setEndTime(1695358700000L)
    executor3Timespan.setStartTime(1695358687000L)
    executor3Timespan.setEndTime(1695358700000L)
    executorMap.put("0", executor0Timespan)
    executorMap.put("1", executor1Timespan)
    executorMap.put("2", executor2Timespan)
    executorMap.put("3", executor3Timespan)

    new AppContext(
      new ApplicationInfo(appId, StartTime, EndTime),
      new AggregateMetrics(),
      mutable.HashMap[String, HostTimeSpan](),
      executorMap,
      jobMap,
      jobSQLExecIDMap,
      mutable.HashMap[Int, StageTimeSpan](),
      mutable.HashMap[Int, Long]())
  }

  def mockcorrectMetrics(csvReaderMock: CsvHadoopReader): CsvHadoopReader = {
    (csvReaderMock.read _).when(s"${csvMetricsPath}/${appId}.driver.jvm.heap.used.csv").returns(jvmHeapDriverCsv)
    (csvReaderMock.read _).when(s"${csvMetricsPath}/${appId}.driver.jvm.heap.usage.csv").returns(jvmHeapUsageDriverCsv)
    (csvReaderMock.read _).when(s"${csvMetricsPath}/${appId}.driver.jvm.heap.max.csv").returns(jvmHeapMaxDriverCsv)
    (csvReaderMock.read _).when(s"${csvMetricsPath}/${appId}.driver.jvm.non-heap.used.csv").returns(jvmNonHeapDriverCsv)

    (csvReaderMock.read _).when(s"${csvMetricsPath}/${appId}.0.jvm.heap.used.csv").returns(jvmHeapExec0Csv)
    (csvReaderMock.read _).when(s"${csvMetricsPath}/${appId}.0.jvm.heap.usage.csv").returns(jvmHeapUsageExec0Csv)
    (csvReaderMock.read _).when(s"${csvMetricsPath}/${appId}.0.jvm.heap.max.csv").returns(jvmHeapMaxExec0Csv)
    (csvReaderMock.read _).when(s"${csvMetricsPath}/${appId}.0.jvm.non-heap.used.csv").returns(jvmNonHeapExec0Csv)
    (csvReaderMock.read _).when(s"${csvMetricsPath}/${appId}.0.executor.cpuTime.csv").returns(cpuTime0Csv)

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

    csvReaderMock
  }

  def mockIncorrectDriverMetrics(csvReader: CsvHadoopReader) = {
    (csvReader.read _).when(s"${csvMetricsPath}/${appId}.driver.jvm.heap.used.csv").returns(jvmHeapDriverCsv)
    (csvReader.read _).when(s"${csvMetricsPath}/${appId}.driver.jvm.heap.usage.csv").returns(jvmHeapUsageDriverCsv)
    (csvReader.read _).when(s"${csvMetricsPath}/${appId}.driver.jvm.heap.max.csv").returns(jvmHeapMaxDriverCsv)
    (csvReader.read _).when(s"${csvMetricsPath}/${appId}.driver.jvm.non-heap.used.csv").returns(emptyFileCsv)
    csvReader
  }

  def getPropertiesLoaderMock(): PropertiesLoader = {
    val propertiesLoaderMock = stub[PropertiesLoader]
    val properties = new Properties()
    properties.setProperty("*.sink.csv.directory", csvMetricsPath)
    (propertiesLoaderMock.load _).when(MetricsPropertiesPath).returns(properties)
    propertiesLoaderMock
  }
}
