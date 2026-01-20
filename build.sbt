ThisBuild / version := "0.1.0-SNAPSHOT"
ThisBuild / scalaVersion := "2.12.18"
ThisBuild / organization := "com.company"

val sparkVersion = "3.5.0"
val hnswlibVersion = "1.1.0"
val json4sVersion = "3.7.0-M11"  // Match Spark 3.5.0's json4s version
val scalatestVersion = "3.2.15"
val akkaVersion = "2.6.20"
val akkaHttpVersion = "10.2.10"

lazy val commonSettings = Seq(
  scalacOptions ++= Seq(
    "-deprecation",
    "-encoding", "UTF-8",
    "-feature",
    "-unchecked"
  ),
  Test / fork := true,
  Test / parallelExecution := false
)

lazy val root = (project in file("."))
  .aggregate(core, sparkIntegration, sparkSqlExtension, apiServer)
  .settings(
    name := "spark-ann",
    publish / skip := true
  )

lazy val core = (project in file("core"))
  .settings(commonSettings)
  .settings(
    name := "spark-ann-core",
    libraryDependencies ++= Seq(
      "com.github.jelmerk" % "hnswlib-core" % hnswlibVersion,
      "org.json4s" %% "json4s-jackson" % json4sVersion,
      "org.scalatest" %% "scalatest" % scalatestVersion % Test
    )
  )

lazy val sparkIntegration = (project in file("spark-integration"))
  .dependsOn(core)
  .settings(commonSettings)
  .settings(
    name := "spark-ann-integration",
    libraryDependencies ++= Seq(
      "org.apache.spark" %% "spark-sql" % sparkVersion % Provided,
      "org.apache.spark" %% "spark-core" % sparkVersion % Provided,
      "org.scalatest" %% "scalatest" % scalatestVersion % Test,
      "org.apache.spark" %% "spark-sql" % sparkVersion % Test,
      "org.apache.spark" %% "spark-core" % sparkVersion % Test,
      "org.json4s" %% "json4s-jackson" % json4sVersion % Test
    )
  )

lazy val sparkSqlExtension = (project in file("spark-sql-extension"))
  .dependsOn(sparkIntegration)
  .settings(commonSettings)
  .settings(
    name := "spark-ann-sql-extension",
    libraryDependencies ++= Seq(
      "org.apache.spark" %% "spark-sql" % sparkVersion % Provided,
      "org.apache.spark" %% "spark-catalyst" % sparkVersion % Provided,
      "org.scalatest" %% "scalatest" % scalatestVersion % Test
    )
  )

lazy val apiServer = (project in file("api-server"))
  .dependsOn(core)
  .settings(commonSettings)
  .settings(
    name := "spark-ann-api-server",
    libraryDependencies ++= Seq(
      "com.typesafe.akka" %% "akka-actor-typed" % akkaVersion,
      "com.typesafe.akka" %% "akka-stream" % akkaVersion,
      "com.typesafe.akka" %% "akka-http" % akkaHttpVersion,
      "com.typesafe.akka" %% "akka-http-spray-json" % akkaHttpVersion,
      "ch.qos.logback" % "logback-classic" % "1.2.11",
      "org.scalatest" %% "scalatest" % scalatestVersion % Test,
      "com.typesafe.akka" %% "akka-actor-testkit-typed" % akkaVersion % Test,
      "com.typesafe.akka" %% "akka-http-testkit" % akkaHttpVersion % Test
    )
  )
