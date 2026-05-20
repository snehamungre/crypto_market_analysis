name := "CryptoMarketAnalysis"
version := "1.1"

scalaVersion := "2.12.15" 

val sparkVersion = "3.3.2" 

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % sparkVersion % "provided",
  "org.apache.spark" %% "spark-sql" % sparkVersion % "provided",
  "org.apache.spark" %% "spark-hive" % sparkVersion % "provided"
)