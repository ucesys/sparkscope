name := "sparkscope-spark3"
organization := "com.ucesys"

scalaVersion := "2.12.18"

spName := "ucesys/sparkscope-spark3"

sparkVersion := "3.2.0"

spAppendScalaVersion := true


libraryDependencies += "org.apache.spark" %% "spark-core" % sparkVersion.value % "provided"
libraryDependencies += "org.apache.spark" %% "spark-sql" % sparkVersion.value % "provided"

libraryDependencies += "org.apache.hadoop" % "hadoop-client" % "2.6.5" % "provided"
libraryDependencies += "org.apache.httpcomponents" % "httpclient" % "4.5.6" % "provided"
libraryDependencies += "org.apache.httpcomponents" % "httpmime" % "4.5.6" % "provided"

libraryDependencies += "org.scalamock" %% "scalamock" % "5.2.0" % Test
libraryDependencies += "org.scalatest" %% "scalatest" % "3.0.5" % Test
libraryDependencies += "com.google.guava" % "guava" % "30.0-jre" % Test


test in assembly := {}

testOptions in Test += Tests.Argument("-oF")

scalacOptions ++= Seq("-target:jvm-1.8")

javacOptions ++= Seq("-source", "1.8", "-target", "1.8")

publishMavenStyle := true


licenses += ("Apache-2.0", url("http://www.apache.org/licenses/LICENSE-2.0"))

credentials += Credentials(Path.userHome / ".ivy2" / ".sbtcredentials")
