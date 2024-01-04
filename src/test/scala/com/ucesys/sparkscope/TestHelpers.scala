package com.ucesys.sparkscope

import com.ucesys.sparkscope.data.DataTable
import com.ucesys.sparkscope.common.{AppContext, SparkScopeLogger}
import com.ucesys.sparkscope.io.metrics.{DriverExecutorMetrics, HadoopMetricReader, MetricReader, MetricReaderFactory}
import com.ucesys.sparkscope.io.property.{PropertiesLoader, PropertiesLoaderFactory}
import com.ucesys.sparkscope.timeline.{ExecutorTimeline, StageTimeline}
import com.ucesys.sparkscope.view.warning.MissingMetricsWarning
import org.apache.spark.SparkConf
import org.scalamock.scalatest.MockFactory
import org.scalatest.FunSuite

import java.io.FileNotFoundException
import java.util.Properties

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
      .set("spark.app.name", "MyApp")

    val sparkScopeConf = new SparkScopeConfLoader()(stub[SparkScopeLogger]).load(sparkConf, getPropertiesLoaderFactoryMock)

    val emptyFileCsv: String =
        """t,value
          |1695358645,0""".stripMargin

    val DriverCsv: String =
        """t,jvm.heap.max,jvm.heap.usage,jvm.heap.used,jvm.non-heap.used
          |1695358645,954728448,0.29708835490780305,283638704,56840944
          |1695358650,954728448,0.23078957002022840,220341368,61081904
          |1695358655,954728448,0.33123242180796525,316237016,66992968
          |1695358660,954728448,0.33957985925648254,324206552,69940328
          |1695358665,954728448,0.29433708882214016,281020200,71093880
          |1695358670,954728448,0.34650217943437670,330815488,72258224
          |1695358675,954728448,0.27892650790688495,266299072,72592544
          |1695358680,954728448,0.22314145603001870,213096240,72947528
          |1695358685,954728448,0.27586009461823430,263432152,72327392
          |1695358690,954728448,0.28044484540173670,267797984,72905864
          |1695358695,954728448,0.29112465705012697,277957632,71930856
          |1695358697,954728448,0.22932481006368840,218993128,70562064
          |1695358700,954728448,0.31738806634994077,303019416,70981360""".stripMargin

    val DriverMetrics: DataTable = DataTable.fromCsv("driver", DriverCsv, ",")

    val Exec1Csv: String =
        """t,jvm.heap.max,jvm.heap.usage,jvm.heap.used,jvm.non-heap.used,executor.cpuTime
          |1695358649,838860800,0.28236982345581050,237607008,42346768,434428446
          |1695358654,838860800,0.11401022911071777,95638712,46358624,2215933296
          |1695358659,838860800,0.37440157890319825,314070808,49276240,4930102712
          |1695358664,838860800,0.44028958320617680,369341672,50268712,7867871200
          |1695358669,838860800,0.32018810272216797,268593248,50243840,11071045636
          |1695358674,838860800,0.41225853919982910,345835736,50339984,13990111255
          |1695358679,838860800,0.13933904647827147,116886064,50620984,17584148484
          |1695358684,838860800,0.40087845802307130,336281224,50314224,20956416798
          |1695358689,838860800,0.42372746467590333,355456568,50847152,23926534546
          |1695358694,838860800,0.13523258209228517,113488816,51045160,27604708924
          |1695358697,838860800,0.21571252822875978,180952784,51120384,29815418742""".stripMargin

    val Exec1Metrics: DataTable = DataTable.fromCsv("1", Exec1Csv, ",")

    val Exec2Csv: String =
        """t,jvm.heap.max,jvm.heap.usage,jvm.heap.used,jvm.non-heap.used,executor.cpuTime
          |1695358649,838860800,0.23338873863220214,195780664,42811920,438401162
          |1695358654,838860800,0.40078392982482910,336201928,46701168,2182426098
          |1695358659,838860800,0.31774411201477050,266543080,49800000,4922506921
          |1695358664,838860800,0.39278621673583985,329492960,50386096,7901965708
          |1695358669,838860800,0.25880491256713867,217101296,50998000,11104348527
          |1695358674,838860800,0.28356681823730470,237873088,51138536,13980744341
          |1695358679,838860800,0.36625462532043457,307236648,51364368,17616604346
          |1695358684,838860800,0.22121253013610840,185639680,50760920,20937026849
          |1695358689,838860800,0.19867253303527832,166666808,50842144,23771909531
          |1695358694,838860800,0.20476229667663573,171814536,50971768,27322915542
          |1695358697,838860800,0.32852493286132810,275586688,50638864,29625862807""".stripMargin

    val Exec2Metrics: DataTable = DataTable.fromCsv("2", Exec2Csv, ",")

    val Exec3Csv: String =
        """t,jvm.heap.max,jvm.heap.usage,jvm.heap.used,jvm.non-heap.used,executor.cpuTime
          |1695358675,838860800,0.36410789489746090,305435840,47486624,2085769750
          |1695358680,838860800,0.31317305564880370,262708600,50236376,5516790869
          |1695358685,838860800,0.20223091125488282,170042672,49463504,8767561125
          |1695358690,838860800,0.17223947525024413,144484944,50636936,11460149659
          |1695358695,838860800,0.28628440856933596,240152768,51004320,15116281483
          |1695358697,838860800,0.20234796524047852,169741776,51158296,18263354541""".stripMargin

    val Exec3Metrics: DataTable = DataTable.fromCsv("3", Exec3Csv, ",")

    val Exec5Csv: String =
        """t,jvm.heap.max,jvm.heap.usage,jvm.heap.used,jvm.non-heap.used,executor.cpuTime
          |1695358691,838860800,0.23783822059631350,199513160,47196664,1877412152
          |1695358696,838860800,0.29748142242431640,249545504,50223304,5451002249
          |1695358697,838860800,0.30450093269348144,255433896,49633008,6629593128""".stripMargin

    val Exec5Metrics: DataTable = DataTable.fromCsv("5", Exec5Csv, ",")

    val Exec7Csv: String =
        """t,jvm.heap.max,jvm.heap.usage,jvm.heap.used,jvm.non-heap.used,executor.cpuTime
          |1695358691,838860800,0.23783822059631350,199513160,47196664,1877412152
          |1695358696,838860800,0.29748142242431640,249545504,50223304,5451002249
          |1695358701,838860800,0.30450093269348144,255433896,49633008,8129593128
          |1695358706,838860800,0.30450093269348144,249545504,50223304,11129593128
          |1695358711,838860800,0.29748142242431640,255433896,49633008,16963300828""".stripMargin

    val Exec7Metrics: DataTable = DataTable.fromCsv("7", Exec7Csv, ",")

    val ExecutorMetricsMap: Map[String, DataTable] = Map(
        "1" -> Exec1Metrics,
        "2" -> Exec2Metrics,
        "3" -> Exec3Metrics,
        "5" -> Exec5Metrics
    )

    val DriverExecutorMetricsMock: DriverExecutorMetrics = DriverExecutorMetrics(
        driverMetrics = DriverMetrics,
        executorMetricsMap = ExecutorMetricsMap
    )

    val missingMetricsWarning = MissingMetricsWarning(Seq("1", "2", "3", "4", "5"), Seq("1", "2", "3", "5"))

    val Stages = Seq(
        stageTimeline(1, 1695358645000L, 1695358671000L, 200),
        stageTimeline(2, 1695358645000L, 1695358700000L, 500),
        stageTimeline(3, 1695358645000L, 1695358660000L, 100),
        stageTimeline(4, 1695358660000L, 1695358671000L, 200),
        stageTimeline(5, 1695358671000L, 1695358700000L, 100),
        stageTimeline(6, 1695358645000L, 1695358655000L, 200),
        stageTimeline(7, 1695358655000L, 1695358700000L, 500),
        stageTimeline(8, 1695358665000L, 1695358680000L, 100),
        stageTimeline(9, 1695358670000L, 1695358675000L, 100),
        stageTimeline(10, 1695358675000L, 1695357200000L, 200),
        stageTimeline(11, 1695358675000L, 1695358671000L, 300),
        stageTimeline(12, 1695358675000L, 1695358700000L, 400),
        stageTimeline(13, 1695358645000L, 1695358660000L, 50),
        stageTimeline(14, 1695358660000L, 1695358671000L, 100),
        stageTimeline(15, 1695358671000L, 1695358700000L, 250),
        stageTimeline(16, 1695358645000L, 1695358655000L, 50),
        stageTimeline(17, 1695358655000L, 1695358700000L, 300),
        stageTimeline(18, 1695358665000L, 1695358680000L, 200),
        stageTimeline(19, 1695358670000L, 1695358675000L, 100),
        stageTimeline(20, 1695358675000L, 1695357200000L, 300),
        stageTimeline(21, 1695358675000L, 1695358671000L, 100),
        stageTimeline(22, 1695358675000L, 1695358700000L, 300),
        stageTimeline(23, 1695358645000L, 1695358660000L, 500),
        stageTimeline(24, 1695358660000L, 1695358671000L, 100),
        stageTimeline(25, 1695358671000L, 1695358700000L, 100),
        stageTimeline(26, 1695358645000L, 1695358655000L, 100),
        stageTimeline(27, 1695358655000L, 1695358700000L, 400),
        stageTimeline(28, 1695358665000L, 1695358680000L, 300),
        stageTimeline(29, 1695358670000L, 1695358675000L, 300),
        stageTimeline(30, 1695358675000L, 1695357200000L, 200),
        stageTimeline(31, 1695358675000L, 1695358671000L, 100),
        stageTimeline(32, 1695358675000L, 1695358700000L, 200),
        stageTimeline(33, 1695358645000L, 1695358660000L, 200),
        stageTimeline(34, 1695358660000L, 1695358671000L, 100),
        stageTimeline(35, 1695358671000L, 1695358700000L, 50),
        stageTimeline(36, 1695358645000L, 1695358655000L, 300),
        stageTimeline(37, 1695358655000L, 1695358700000L, 400),
        stageTimeline(38, 1695358665000L, 1695358680000L, 200),
        stageTimeline(39, 1695358670000L, 1695358675000L, 100),
        stageTimeline(40, 1695358675000L, 1695357200000L, 100)
    )

    def stageTimeline(stageId: Int, start: Long, end: Long, numTasks: Long, parentsStages: Seq[Int] = Seq.empty): StageTimeline = {
        new StageTimeline(stageId, Some(start), numTasks, parentsStages, Some(end))
    }

    def getAppId: String = s"app-${System.currentTimeMillis()}"

    def mockAppContext(appName: String): AppContext = {
        val executorMap: Map[String, ExecutorTimeline] = Map(
            "1" -> ExecutorTimeline("1", "1",1, 1695358645000L, Some(1695358700000L)),
            "2" -> ExecutorTimeline("2", "2",1, 1695358645000L, Some(1695358700000L)),
            "3" -> ExecutorTimeline("3", "3",1, 1695358671000L, Some(1695358700000L)),
            "5" -> ExecutorTimeline("5", "5",1, 1695358687000L, Some(1695358700000L))
        )

        AppContext(
            s"${getAppId}-${appName}",
            StartTime,
            Some(EndTime),
            executorMap,
            Stages
        )
    }

    def mockAppContextExecutorsNotRemoved(appName: String): AppContext = {
        val executorMap: Map[String, ExecutorTimeline] = Map(
            "1" -> ExecutorTimeline("1", "1",1, 1695358645000L, None),
            "2" -> ExecutorTimeline("2", "2",1, 1695358645000L, None),
            "3" -> ExecutorTimeline("3", "3",1, 1695358671000L, None),
            "5" -> ExecutorTimeline("5", "5",1, 1695358687000L, None)
        )

        AppContext(
            s"${getAppId}${appName}",
            StartTime,
            Some(EndTime),
            executorMap,
            Stages
        )
    }

    def mockAppContextMissingExecutorMetrics(appName: String): AppContext = {
        mockAppContext(appName).copy(
            executorMap = Map(
                "1" -> ExecutorTimeline("1", "1", 1, 1695358645000L, Some(1695358700000L)),
                "2" -> ExecutorTimeline("2", "2", 1, 1695358645000L, Some(1695358700000L)),
                "3" -> ExecutorTimeline("3", "3", 1, 1695358671000L, Some(1695358700000L)),
                "5" -> ExecutorTimeline("5", "5", 1, 1695358687000L, Some(1695358700000L)),
                "6" -> ExecutorTimeline("6", "6", 1, 1695358687000L, Some(1695358700000L))
            )
        )
    }

    def mockAppContextWithDownscaling(appName: String): AppContext = {
        val executorMap: Map[String, ExecutorTimeline] = Map(
            "1" -> ExecutorTimeline("1", "1", 1, 1695358645000L, Some(1695358700000L)),
            "2" -> ExecutorTimeline("2", "2", 1, 1695358645000L, Some(1695358700000L)),
            "3" -> ExecutorTimeline("3", "3", 1, 1695358671000L, Some(1695358700000L)),
            "5" -> ExecutorTimeline("5", "5", 1, 1695358687000L, Some(1695358700000L)),
            "7" -> ExecutorTimeline("7", "7", 1, 1695358687000L, Some(1695358715000L))
        )

        AppContext(
            s"${getAppId}${appName}",
            StartTime,
            Some(EndTime),
            executorMap,
            Stages
        )
    }

    def mockAppContextWithDownscalingMuticore(appName: String, appId: String = getAppId): AppContext = {
        val executorMap: Map[String, ExecutorTimeline] = Map(
            "1" -> ExecutorTimeline("1", "1", 2, 1695358645000L, Some(1695358700000L)),
            "2" -> ExecutorTimeline("2", "2", 2, 1695358645000L, Some(1695358700000L)),
            "3" -> ExecutorTimeline("3", "3", 2, 1695358671000L, Some(1695358700000L)),
            "5" -> ExecutorTimeline("5", "5", 2, 1695358687000L, Some(1695358700000L)),
            "7" -> ExecutorTimeline("7", "7", 2, 1695358687000L, Some(1695358715000L))
        )

        AppContext(
            s"${appId}${appName}",
            StartTime,
            Some(EndTime),
            executorMap,
            Seq.empty
        )
    }

    def mockcorrectMetrics(csvReaderMock: HadoopMetricReader, appId: String): HadoopMetricReader = {
        (csvReaderMock.readDriver _).when().returns(DriverMetrics)

        (csvReaderMock.readExecutor _).when("1").returns(Exec1Metrics)
        (csvReaderMock.readExecutor _).when("2").returns(Exec2Metrics)
        (csvReaderMock.readExecutor _).when("3").returns(Exec3Metrics)
        (csvReaderMock.readExecutor _).when("5").returns(Exec5Metrics)
        (csvReaderMock.readExecutor _).when("6").throws(new FileNotFoundException)

        csvReaderMock
    }

    def mockMetricsWithDownscaling(csvReaderMock: HadoopMetricReader, appId: String): HadoopMetricReader = {
        mockcorrectMetrics(csvReaderMock, appId)
        (csvReaderMock.readExecutor _).when("7").returns(Exec7Metrics)
        csvReaderMock
    }

    def mockMetricsEventLog(csvReaderMock: HadoopMetricReader, appId: String): HadoopMetricReader = {
        val DriverCsv: String =
        """t,jvm.heap.max,jvm.heap.usage,jvm.heap.used,jvm.non-heap.used
        |1698236101,1073741824,0.12747859954833984,136879104,89620432
        |1698236104,1073741824,0.10080626606941223,108239904,93666616""".stripMargin

        val Exec0Csv: String =
        """t,jvm.heap.max,jvm.heap.usage,jvm.heap.used,jvm.non-heap.used,executor.cpuTime
        |1698236103,943718400,0.07733701917860243,74032944,67918808,4076184096
        |1698236104,943718400,0.07759124755859376,73224288,68396480,5137414040""".stripMargin

        val Exec1Csv: String =
        """t,jvm.heap.max,jvm.heap.usage,jvm.heap.used,jvm.non-heap.used,executor.cpuTime
        |1698236103,943718400,0.11500205993652343,108529560,68071032,4206969830
        |1698236104,943718400,0.09192616780598958,86752416,68522480,5259304929""".stripMargin

        (csvReaderMock.readDriver _).when().returns(DataTable.fromCsv("driver", DriverCsv, ","))
        (csvReaderMock.readExecutor _).when("0").returns(DataTable.fromCsv("0", Exec0Csv, ","))
        (csvReaderMock.readExecutor _).when("1").returns(DataTable.fromCsv("1", Exec1Csv, ","))

        csvReaderMock
    }

    def mockMetricsEventLogStages(csvReaderMock: HadoopMetricReader, appId: String): HadoopMetricReader = {
        val DriverCsv: String =
        """t,jvm.heap.max,jvm.heap.usage,jvm.heap.used,jvm.non-heap.used
        |1700654082,1073741824,0.12747859954833984,136879104,89620432
        |1700654099,1073741824,0.10080626606941223,108239904,93666616""".stripMargin

        val Exec0Csv: String =
        """t,jvm.heap.max,jvm.heap.usage,jvm.heap.used,jvm.non-heap.used,executor.cpuTime
        |1700654082,943718400,0.07733701917860243,74032944,67918808,4076184096
        |1700654099,943718400,0.07759124755859376,73224288,68396480,5137414040""".stripMargin

        val Exec1Csv: String =
        """t,jvm.heap.max,jvm.heap.usage,jvm.heap.used,jvm.non-heap.used,executor.cpuTime
        |1700654082,943718400,0.11500205993652343,108529560,68071032,4206969830
        |1700654099,943718400,0.09192616780598958,86752416,68522480,5259304929""".stripMargin

        (csvReaderMock.readDriver _).when().returns(DataTable.fromCsv("driver", DriverCsv, ","))
        (csvReaderMock.readExecutor _).when("0").returns(DataTable.fromCsv("0", Exec0Csv, ","))
        (csvReaderMock.readExecutor _).when("1").returns(DataTable.fromCsv("1", Exec1Csv, ","))

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
