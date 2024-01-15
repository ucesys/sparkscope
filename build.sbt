name := "sparkscope-spark2"
organization := "com.ucesys"

scalaVersion := "2.11.12"
sparkVersion := "2.4.0"

spAppendScalaVersion := true

libraryDependencies += "org.apache.spark" %% "spark-core" % sparkVersion.value % "provided"
libraryDependencies += "org.apache.spark" %% "spark-sql" % sparkVersion.value % "provided"

libraryDependencies += "org.apache.hadoop" % "hadoop-client" % "2.6.5" % "provided"
libraryDependencies += "org.apache.httpcomponents" % "httpclient" % "4.5.13" % "provided"
libraryDependencies += "org.apache.httpcomponents" % "httpmime" % "4.5.13" % "provided"
libraryDependencies += "com.amazonaws" % "aws-java-sdk" % "1.11.1034" % "provided"

libraryDependencies += "org.scalamock" %% "scalamock" % "5.2.0" % Test
libraryDependencies += "org.scalatest" %% "scalatest" % "3.1.2" % Test
libraryDependencies += "org.mockito" %% "mockito-scala" % "1.14.8" % Test
libraryDependencies += "com.google.guava" % "guava" % "30.0-jre" % Test

test in assembly := {}

testOptions in Test += Tests.Argument("-oF")

scalacOptions ++= Seq("-target:jvm-1.8")

javacOptions ++= Seq("-source", "1.8", "-target", "1.8")

publishMavenStyle := true

licenses += ("Apache-2.0", url("http://www.apache.org/licenses/LICENSE-2.0"))

credentials += Credentials(Path.userHome / ".ivy2" / ".sbtcredentials")
