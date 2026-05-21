name := "CryptoMarketAnalysis"
version := "1.1"

scalaVersion := "2.13.17"          

val sparkVersion = "3.5.3"         

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % sparkVersion % "provided",
  "org.apache.spark" %% "spark-sql"  % sparkVersion % "provided",
  "org.apache.spark" %% "spark-hive" % sparkVersion % "provided"
)