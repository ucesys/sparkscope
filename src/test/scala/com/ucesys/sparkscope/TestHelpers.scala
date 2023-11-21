package com.ucesys.sparkscope

import com.ucesys.sparklens.common.{AggregateMetrics, AppContext, ApplicationInfo}
import com.ucesys.sparklens.timespan.{ExecutorTimeSpan, HostTimeSpan, JobTimeSpan, StageTimeSpan}
import com.ucesys.sparkscope.data.DataTable
import com.ucesys.sparkscope.io._
import com.ucesys.sparkscope.common.{ExecutorContext, SparkScopeContext, SparkScopeLogger, StageContext}
import com.ucesys.sparkscope.io.metrics.{DriverExecutorMetrics, HadoopMetricReader, MetricReader, MetricReaderFactory}
import com.ucesys.sparkscope.io.property.{PropertiesLoader, PropertiesLoaderFactory}
import com.ucesys.sparkscope.warning.MissingMetricsWarning
import org.apache.spark.SparkConf
import org.scalamock.scalatest.MockFactory
import org.scalatest.FunSuite

import java.io.FileNotFoundException
import java.util.Properties
import scala.collection.mutable

object TestHelpers extends FunSuite with MockFactory {
    val TestDir = ".tests"
    val appId = "app-20230101010819-test"
    val StartTime: Long = 1695358644000L
    val EndTime: Long = 1695358700000L
    val MetricsPropertiesPath = "path/to/metrics.properties"
    val csvMetricsPath = "/tmp/csv-metrics"
    val sparkConf = new SparkConf()
      .set("spark.metrics.conf", MetricsPropertiesPath)
      .set("spark.sparkscope.html.path", "/path/to/html/report")

    val sparkScopeConf = new SparkScopeConfLoader()(stub[SparkScopeLogger]).load(sparkConf, getPropertiesLoaderFactoryMock)

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
          |1695358697,18263354541""".stripMargin

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
          |1695358697,6629593128""".stripMargin

    val jvmHeapExec7Csv: String =
        """t,value
          |1695358691,199513160
          |1695358696,249545504
          |1695358701,255433896
          |1695358706,249545504
          |1695358711,255433896""".stripMargin

    val jvmHeapUsageExec7Csv: String =
        """t,value
          |1695358691,0.2378382205963135
          |1695358696,0.2974814224243164
          |1695358701,0.30450093269348144
          |1695358706,0.30450093269348144
          |1695358711,0.2974814224243164""".stripMargin

    val jvmHeapMaxExec7Csv: String =
        """t,value
          |1695358691,838860800
          |1695358696,838860800
          |1695358701,838860800
          |1695358706,838860800
          |1695358711,838860800""".stripMargin

    val jvmNonHeapExec7Csv: String =
        """t,value
          |1695358691,47196664
          |1695358696,50223304
          |1695358701,49633008
          |1695358706,50223304
          |1695358711,49633008""".stripMargin

    val cpuTime7Csv: String =
        """t,count
          |1695358691,1877412152
          |1695358696,5451002249
          |1695358701,8129593128
          |1695358706,11129593128
          |1695358711,16963300828""".stripMargin

    val driverMetrics = Seq(
        DataTable.fromCsv("driver-heap-used", jvmHeapDriverCsv, ",", Seq("t", JvmHeapUsed.name)),
        DataTable.fromCsv("driver-heap-max", jvmHeapMaxDriverCsv, ",", Seq("t", JvmHeapMax.name)),
        DataTable.fromCsv("driver-heap-usage", jvmHeapUsageDriverCsv, ",", Seq("t", JvmHeapUsage.name)),
        DataTable.fromCsv("driver-non-heap", jvmNonHeapDriverCsv, ",", Seq("t", JvmNonHeapUsed.name))
    )

    val executorMetricsMap = Map(
        "1" -> Seq(
            DataTable.fromCsv("exec1-heap-used", jvmHeapExec1Csv, ",", Seq("t", JvmHeapUsed.name)),
            DataTable.fromCsv("exec1-heap-max", jvmHeapMaxExec1Csv, ",", Seq("t", JvmHeapMax.name)),
            DataTable.fromCsv("exec1-heap-usage", jvmHeapUsageExec1Csv, ",", Seq("t", JvmHeapUsage.name)),
            DataTable.fromCsv("exec1-non-heap", jvmNonHeapExec1Csv, ",", Seq("t", JvmNonHeapUsed.name)),
            DataTable.fromCsv("exec1-cpu-time", cpuTime1Csv, ",", Seq("t", CpuTime.name))
        ),
        "2" -> Seq(
            DataTable.fromCsv("exec2-heap-used", jvmHeapExec2Csv, ",", Seq("t", JvmHeapUsed.name)),
            DataTable.fromCsv("exec2-heap-max", jvmHeapMaxExec2Csv, ",", Seq("t", JvmHeapMax.name)),
            DataTable.fromCsv("exec2-heap-usage", jvmHeapUsageExec2Csv, ",", Seq("t", JvmHeapUsage.name)),
            DataTable.fromCsv("exec2-non-heap", jvmNonHeapExec2Csv, ",", Seq("t", JvmNonHeapUsed.name)),
            DataTable.fromCsv("exec2-cpu-time", cpuTime2Csv, ",", Seq("t", CpuTime.name))
        ),
        "3" -> Seq(
            DataTable.fromCsv("exec3-heap-used", jvmHeapExec3Csv, ",", Seq("t", JvmHeapUsed.name)),
            DataTable.fromCsv("exec3-heap-max", jvmHeapMaxExec3Csv, ",", Seq("t", JvmHeapMax.name)),
            DataTable.fromCsv("exec3-heap-usage", jvmHeapUsageExec3Csv, ",", Seq("t", JvmHeapUsage.name)),
            DataTable.fromCsv("exec3-non-heap", jvmNonHeapExec3Csv, ",", Seq("t", JvmNonHeapUsed.name)),
            DataTable.fromCsv("exec3-cpu-time", cpuTime3Csv, ",", Seq("t", CpuTime.name))
        ),
        "5" -> Seq(
            DataTable.fromCsv("exec5-heap-used", jvmHeapExec5Csv, ",", Seq("t", JvmHeapUsed.name)),
            DataTable.fromCsv("exec5-heap-max", jvmHeapMaxExec5Csv, ",", Seq("t", JvmHeapMax.name)),
            DataTable.fromCsv("exec5-heap-usage", jvmHeapUsageExec5Csv, ",", Seq("t", JvmHeapUsage.name)),
            DataTable.fromCsv("exec5-non-heap", jvmNonHeapExec5Csv, ",", Seq("t", JvmNonHeapUsed.name)),
            DataTable.fromCsv("exec5-cpu-time", cpuTime5Csv, ",", Seq("t", CpuTime.name))
        )
    )

    val DriverExecutorMetricsMock: DriverExecutorMetrics = DriverExecutorMetrics(
        driverMetrics = driverMetrics,
        executorMetricsMap = executorMetricsMap
    )

    val missingMetricsWarning = MissingMetricsWarning(Seq("1", "2", "3", "4", "5"), Seq("1", "2", "3", "5"))

    def getAppId: String = s"app-${System.currentTimeMillis()}"

    def mockAppContext(appName: String): SparkScopeContext = {
        val executorMap: Map[String, ExecutorContext] = Map(
            "1" -> ExecutorContext("1", 1, 1695358645000L, Some(1695358700000L)),
            "2" -> ExecutorContext("2", 1, 1695358645000L, Some(1695358700000L)),
            "3" -> ExecutorContext("3", 1, 1695358671000L, Some(1695358700000L)),
            "5" -> ExecutorContext("5", 1, 1695358687000L, Some(1695358700000L))
        )

        val stages = Seq(
            StageContext("1", 1695358645L, 1695358671L, 200),
            StageContext("2", 1695358645L, 1695358700L, 500),
            StageContext("3", 1695358645L, 1695358660L, 100),
            StageContext("4", 1695358660L, 1695358671L, 200),
            StageContext("5", 1695358671L, 1695358700L, 100),
            StageContext("6", 1695358645L, 1695358655L, 200),
            StageContext("7", 1695358655L, 1695358700L, 500),
            StageContext("8", 1695358665L, 1695358680L, 100),
            StageContext("9", 1695358670L, 1695358675L, 200),
            StageContext("10", 1695358675L, 1695357200L, 100)
        )

        SparkScopeContext(
            s"${getAppId}-${appName}",
            StartTime,
            Some(EndTime),
            executorMap,
            stages
        )
    }

    def mockAppContextExecutorsNotRemoved(appName: String): SparkScopeContext = {
        val executorMap: Map[String, ExecutorContext] = Map(
            "1" -> ExecutorContext("1", 1, 1695358645000L, None),
            "2" -> ExecutorContext("2", 1, 1695358645000L, None),
            "3" -> ExecutorContext("3", 1, 1695358671000L, None),
            "5" -> ExecutorContext("5", 1, 1695358687000L, None)
        )

        SparkScopeContext(
            s"${getAppId}${appName}",
            StartTime,
            Some(EndTime),
            executorMap,
            Seq.empty
        )
    }

    def mockAppContextMissingExecutorMetrics(appName: String): SparkScopeContext = {
        mockAppContext(appName).copy(
            executorMap = Map(
                "1" -> ExecutorContext("1", 1, 1695358645000L, Some(1695358700000L)),
                "2" -> ExecutorContext("2", 1, 1695358645000L, Some(1695358700000L)),
                "3" -> ExecutorContext("3", 1, 1695358671000L, Some(1695358700000L)),
                "5" -> ExecutorContext("5", 1, 1695358687000L, Some(1695358700000L)),
                "6" -> ExecutorContext("6", 1, 1695358687000L, Some(1695358700000L))
            )
        )
    }

    def mockAppContextWithDownscaling(appName: String): SparkScopeContext = {
        val executorMap: Map[String, ExecutorContext] = Map(
            "1" -> ExecutorContext("1", 1, 1695358645000L, Some(1695358700000L)),
            "2" -> ExecutorContext("2", 1, 1695358645000L, Some(1695358700000L)),
            "3" -> ExecutorContext("3", 1, 1695358671000L, Some(1695358700000L)),
            "5" -> ExecutorContext("5", 1, 1695358687000L, Some(1695358700000L)),
            "7" -> ExecutorContext("7", 1, 1695358687000L, Some(1695358715000L))
        )

        SparkScopeContext(
            s"${getAppId}${appName}",
            StartTime,
            Some(EndTime),
            executorMap,
            Seq.empty
        )
    }

    def mockAppContextWithDownscalingMuticore(appName: String, appId: String = getAppId): SparkScopeContext = {
        val executorMap: Map[String, ExecutorContext] = Map(
            "1" -> ExecutorContext("1", 2, 1695358645000L, Some(1695358700000L)),
            "2" -> ExecutorContext("2", 2, 1695358645000L, Some(1695358700000L)),
            "3" -> ExecutorContext("3", 2, 1695358671000L, Some(1695358700000L)),
            "5" -> ExecutorContext("5", 2, 1695358687000L, Some(1695358700000L)),
            "7" -> ExecutorContext("7", 2, 1695358687000L, Some(1695358715000L))
        )

        SparkScopeContext(
            s"${appId}${appName}",
            StartTime,
            Some(EndTime),
            executorMap,
            Seq.empty
        )
    }

    def mockcorrectMetrics(csvReaderMock: HadoopMetricReader, appId: String): HadoopMetricReader = {
        (csvReaderMock.readDriver _).when(JvmHeapUsed).returns(DataTable.fromCsv("", jvmHeapDriverCsv, ",", Seq("t", JvmHeapUsed.name)))
        (csvReaderMock.readDriver _).when(JvmHeapUsage).returns(DataTable.fromCsv("", jvmHeapUsageDriverCsv, ",", Seq("t", JvmHeapUsage.name)))
        (csvReaderMock.readDriver _).when(JvmHeapMax).returns(DataTable.fromCsv("", jvmHeapMaxDriverCsv, ",", Seq("t", JvmHeapMax.name)))
        (csvReaderMock.readDriver _).when(JvmNonHeapUsed).returns(DataTable.fromCsv("", jvmNonHeapDriverCsv, ",", Seq("t", JvmNonHeapUsed.name)))

        (csvReaderMock.readExecutor _).when(JvmHeapUsed, "1").returns(DataTable.fromCsv("", jvmHeapExec1Csv, ",", Seq("t", JvmHeapUsed.name)))
        (csvReaderMock.readExecutor _).when(JvmHeapUsage, "1").returns(DataTable.fromCsv("", jvmHeapUsageExec1Csv, ",", Seq("t", JvmHeapUsage.name)))
        (csvReaderMock.readExecutor _).when(JvmHeapMax, "1").returns(DataTable.fromCsv("", jvmHeapMaxExec1Csv, ",", Seq("t", JvmHeapMax.name)))
        (csvReaderMock.readExecutor _).when(JvmNonHeapUsed, "1").returns(DataTable.fromCsv("", jvmNonHeapExec1Csv, ",", Seq("t", JvmNonHeapUsed.name)))
        (csvReaderMock.readExecutor _).when(CpuTime, "1").returns(DataTable.fromCsv("", cpuTime1Csv, ",", Seq("t", CpuTime.name)))

        (csvReaderMock.readExecutor _).when(JvmHeapUsed, "2").returns(DataTable.fromCsv("", jvmHeapExec2Csv, ",", Seq("t", JvmHeapUsed.name)))
        (csvReaderMock.readExecutor _).when(JvmHeapUsage, "2").returns(DataTable.fromCsv("", jvmHeapUsageExec2Csv, ",", Seq("t", JvmHeapUsage.name)))
        (csvReaderMock.readExecutor _).when(JvmHeapMax, "2").returns(DataTable.fromCsv("", jvmHeapMaxExec2Csv, ",", Seq("t", JvmHeapMax.name)))
        (csvReaderMock.readExecutor _).when(JvmNonHeapUsed, "2").returns(DataTable.fromCsv("", jvmNonHeapExec2Csv, ",", Seq("t", JvmNonHeapUsed.name)))
        (csvReaderMock.readExecutor _).when(CpuTime, "2").returns(DataTable.fromCsv("", cpuTime2Csv, ",", Seq("t", CpuTime.name)))

        (csvReaderMock.readExecutor _).when(JvmHeapUsed, "3").returns(DataTable.fromCsv("", jvmHeapExec3Csv, ",", Seq("t", JvmHeapUsed.name)))
        (csvReaderMock.readExecutor _).when(JvmHeapUsage, "3").returns(DataTable.fromCsv("", jvmHeapUsageExec3Csv, ",", Seq("t", JvmHeapUsage.name)))
        (csvReaderMock.readExecutor _).when(JvmHeapMax, "3").returns(DataTable.fromCsv("", jvmHeapMaxExec3Csv, ",", Seq("t", JvmHeapMax.name)))
        (csvReaderMock.readExecutor _).when(JvmNonHeapUsed, "3").returns(DataTable.fromCsv("", jvmNonHeapExec3Csv, ",", Seq("t", JvmNonHeapUsed.name)))
        (csvReaderMock.readExecutor _).when(CpuTime, "3").returns(DataTable.fromCsv("", cpuTime3Csv, ",", Seq("t", CpuTime.name)))

        // Missing one ide on purpose so that order of executor ids is not assumed
        (csvReaderMock.readExecutor _).when(JvmHeapUsed, "5").returns(DataTable.fromCsv("", jvmHeapExec5Csv, ",", Seq("t", JvmHeapUsed.name)))
        (csvReaderMock.readExecutor _).when(JvmHeapUsage, "5").returns(DataTable.fromCsv("", jvmHeapUsageExec5Csv, ",", Seq("t", JvmHeapUsage.name)))
        (csvReaderMock.readExecutor _).when(JvmHeapMax, "5").returns(DataTable.fromCsv("", jvmHeapMaxExec5Csv, ",", Seq("t", JvmHeapMax.name)))
        (csvReaderMock.readExecutor _).when(JvmNonHeapUsed, "5").returns(DataTable.fromCsv("", jvmNonHeapExec5Csv, ",", Seq("t", JvmNonHeapUsed.name)))
        (csvReaderMock.readExecutor _).when(CpuTime, "5").returns(DataTable.fromCsv("", cpuTime5Csv, ",", Seq("t", CpuTime.name)))

        (csvReaderMock.readExecutor _).when(JvmHeapUsed, "6").throws(new FileNotFoundException)
        (csvReaderMock.readExecutor _).when(JvmHeapUsage, "6").throws(new FileNotFoundException)
        (csvReaderMock.readExecutor _).when(JvmHeapMax, "6").throws(new FileNotFoundException)
        (csvReaderMock.readExecutor _).when(JvmNonHeapUsed, "6").throws(new FileNotFoundException)
        (csvReaderMock.readExecutor _).when(CpuTime, "6").throws(new FileNotFoundException)

        csvReaderMock
    }

    def mockMetricsWithDownscaling(csvReaderMock: HadoopMetricReader, appId: String): HadoopMetricReader = {
        mockcorrectMetrics(csvReaderMock, appId)

        (csvReaderMock.readExecutor _).when(JvmHeapUsed, "7").returns(DataTable.fromCsv("", jvmHeapExec7Csv, ",", Seq("t", JvmHeapUsed.name)))
        (csvReaderMock.readExecutor _).when(JvmHeapUsage, "7").returns(DataTable.fromCsv("", jvmHeapUsageExec7Csv, ",", Seq("t", JvmHeapUsage.name)))
        (csvReaderMock.readExecutor _).when(JvmHeapMax, "7").returns(DataTable.fromCsv("", jvmHeapMaxExec7Csv, ",", Seq("t", JvmHeapMax.name)))
        (csvReaderMock.readExecutor _).when(JvmNonHeapUsed, "7").returns(DataTable.fromCsv("", jvmNonHeapExec7Csv, ",", Seq("t", JvmNonHeapUsed.name)))
        (csvReaderMock.readExecutor _).when(CpuTime, "7").returns(DataTable.fromCsv("", cpuTime7Csv, ",", Seq("t", CpuTime.name)))

        csvReaderMock
    }

    def mockIncorrectDriverMetrics(csvReader: HadoopMetricReader, appId: String): HadoopMetricReader = {

        val driverNonHeapRows = jvmNonHeapDriverCsv.split("\n").toSeq
        val jvmNonHeapRowsWithoutLast = driverNonHeapRows.filterNot(_ == driverNonHeapRows.last).mkString("\n")
        (csvReader.readDriver _).when(JvmHeapUsed).returns(DataTable.fromCsv("", jvmHeapDriverCsv, ",", Seq("t", JvmHeapUsed.name)))
        (csvReader.readDriver _).when(JvmHeapUsage).returns(DataTable.fromCsv("", jvmHeapUsageDriverCsv, ",", Seq("t", JvmHeapUsage.name)))
        (csvReader.readDriver _).when(JvmHeapMax).returns(DataTable.fromCsv("", jvmHeapMaxDriverCsv, ",", Seq("t", JvmHeapMax.name)))
        (csvReader.readDriver _).when(JvmNonHeapUsed).returns(DataTable.fromCsv("", jvmNonHeapRowsWithoutLast, ",", Seq("t", JvmNonHeapUsed.name)))

        val executorCpuRows = cpuTime1Csv.split("\n").toSeq
        val executorCpuRowsWithoutLast = executorCpuRows.filterNot(_ == executorCpuRows.last).mkString("\n")
        (csvReader.readExecutor _).when(JvmHeapUsed, "1").returns(DataTable.fromCsv("", jvmHeapExec1Csv, ",", Seq("t", JvmHeapUsed.name)))
        (csvReader.readExecutor _).when(JvmHeapUsage, "1").returns(DataTable.fromCsv("", jvmHeapUsageExec1Csv, ",", Seq("t", JvmHeapUsage.name)))
        (csvReader.readExecutor _).when(JvmHeapMax, "1").returns(DataTable.fromCsv("", jvmHeapMaxExec1Csv, ",", Seq("t", JvmHeapMax.name)))
        (csvReader.readExecutor _).when(JvmNonHeapUsed, "1").returns(DataTable.fromCsv("", jvmNonHeapExec1Csv, ",", Seq("t", JvmNonHeapUsed.name)))
        (csvReader.readExecutor _).when(CpuTime, "1").returns(DataTable.fromCsv("", executorCpuRowsWithoutLast, ",", Seq("t", CpuTime.name)))

        csvReader
    }

    def mockMetricsEventLog(csvReaderMock: HadoopMetricReader, appId: String): HadoopMetricReader = {
        val jvmHeapDriverCsv: String =
            """t,value
              |1698236101,136879104
              |1698236104,108239904""".stripMargin

        val jvmHeapUsageDriverCsv: String =
            """t,value
              |1698236101,0.12747859954833984
              |1698236104,0.10080626606941223""".stripMargin

        val jvmHeapMaxDriverCsv: String =
            """t,value
              |1698236101,1073741824
              |1698236104,1073741824""".stripMargin

        val jvmNonHeapDriverCsv: String =
            """t,value
              |1698236101,89620432
              |1698236104,93666616""".stripMargin

        (csvReaderMock.readDriver _).when(JvmHeapUsed).returns(DataTable.fromCsv("", jvmHeapDriverCsv, ",", Seq("t", JvmHeapUsed.name)))
        (csvReaderMock.readDriver _).when(JvmHeapUsage).returns(DataTable.fromCsv("", jvmHeapUsageDriverCsv, ",", Seq("t", JvmHeapUsage.name)))
        (csvReaderMock.readDriver _).when(JvmHeapMax).returns(DataTable.fromCsv("", jvmHeapMaxDriverCsv, ",", Seq("t", JvmHeapMax.name)))
        (csvReaderMock.readDriver _).when(JvmNonHeapUsed).returns(DataTable.fromCsv("", jvmNonHeapDriverCsv, ",", Seq("t", JvmNonHeapUsed.name)))

        val jvmHeapExec0Csv: String =
            """t,value
              |1698236103,74032944
              |1698236104,73224288""".stripMargin

        val jvmHeapUsageExec0Csv: String =
            """t,value
              |1698236103,0.07733701917860243
              |1698236104,0.07759124755859376""".stripMargin

        val jvmHeapMaxExec0Csv: String =
            """t,value
              |1698236103,943718400
              |1698236104,943718400""".stripMargin

        val jvmNonHeapExec0Csv: String =
            """t,value
              |1698236103,67918808
              |1698236104,68396480""".stripMargin

        val jvmCpuTimeExec0Csv: String =
            """t,value
              |1698236103,4076184096
              |1698236104,5137414040""".stripMargin

        val jvmHeapExec1Csv: String =
            """t,value
              |1698236103,108529560
              |1698236104,86752416""".stripMargin

        val jvmHeapUsageExec1Csv: String =
            """t,value
              |1698236103,0.11500205993652343
              |1698236104,0.09192616780598958""".stripMargin

        val jvmHeapMaxExec1Csv: String =
            """t,value
              |1698236103,943718400
              |1698236104,943718400""".stripMargin

        val jvmNonHeapExec1Csv: String =
            """t,value
              |1698236103,68071032
              |1698236104,68522480""".stripMargin

        val jvmCpuTimeExec1Csv: String =
            """t,value
              |1698236103,4206969830
              |1698236104,5259304929""".stripMargin


        (csvReaderMock.readExecutor _).when(JvmHeapUsed, "0").returns(DataTable.fromCsv("", jvmHeapExec0Csv, ",", Seq("t", JvmHeapUsed.name)))
        (csvReaderMock.readExecutor _).when(JvmHeapUsage, "0").returns(DataTable.fromCsv("", jvmHeapUsageExec0Csv, ",", Seq("t", JvmHeapUsage.name)))
        (csvReaderMock.readExecutor _).when(JvmHeapMax, "0").returns(DataTable.fromCsv("", jvmHeapMaxExec0Csv, ",", Seq("t", JvmHeapMax.name)))
        (csvReaderMock.readExecutor _).when(JvmNonHeapUsed, "0").returns(DataTable.fromCsv("", jvmNonHeapExec0Csv, ",", Seq("t", JvmNonHeapUsed.name)))
        (csvReaderMock.readExecutor _).when(CpuTime, "0").returns(DataTable.fromCsv("", jvmCpuTimeExec0Csv, ",", Seq("t", CpuTime.name)))

        (csvReaderMock.readExecutor _).when(JvmHeapUsed, "1").returns(DataTable.fromCsv("", jvmHeapExec1Csv, ",", Seq("t", JvmHeapUsed.name)))
        (csvReaderMock.readExecutor _).when(JvmHeapUsage, "1").returns(DataTable.fromCsv("", jvmHeapUsageExec1Csv, ",", Seq("t", JvmHeapUsage.name)))
        (csvReaderMock.readExecutor _).when(JvmHeapMax, "1").returns(DataTable.fromCsv("", jvmHeapMaxExec1Csv, ",", Seq("t", JvmHeapMax.name)))
        (csvReaderMock.readExecutor _).when(JvmNonHeapUsed, "1").returns(DataTable.fromCsv("", jvmNonHeapExec1Csv, ",", Seq("t", JvmNonHeapUsed.name)))
        (csvReaderMock.readExecutor _).when(CpuTime, "1").returns(DataTable.fromCsv("", jvmCpuTimeExec1Csv, ",", Seq("t", CpuTime.name)))

        csvReaderMock
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

    def getFileReaderFactoryMock(reader: MetricReader): MetricReaderFactory = {
        val fileReaderFactoryMock = stub[MetricReaderFactory]
        implicit val logger: SparkScopeLogger = mock[SparkScopeLogger]
        (fileReaderFactoryMock.getMetricReader _).when(*, *).returns(reader)
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
