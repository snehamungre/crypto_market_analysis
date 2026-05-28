name := "CryptoMarketAnalysis"
version := "1.1"

scalaVersion := "2.13.17"          

val sparkVersion = "3.5.3"         

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % sparkVersion % "provided",
  "org.apache.spark" %% "spark-sql"  % sparkVersion % "provided",
  "org.apache.spark" %% "spark-hive" % sparkVersion % "provided",
  "org.scalatest"    %% "scalatest"  % "3.2.19"     % Test,
  "com.holdenkarau"  %% "spark-testing-base" % "3.5.3_2.0.1" % Test
)

// needed for spark-testing-base
Test / fork := true
Test / javaOptions ++= Seq(
  "--add-exports=java.base/sun.nio.ch=ALL-UNNAMED",
  "--add-opens=java.base/java.nio=ALL-UNNAMED",
  "--add-opens=java.base/sun.nio.ch=ALL-UNNAMED",
  "--add-opens=java.base/java.lang=ALL-UNNAMED",
  "--add-opens=java.base/java.util=ALL-UNNAMED"
)