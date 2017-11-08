name := "scala_spark_project_template"
version := "1.0"
scalaVersion := "2.11.11"

libraryDependencies += "org.apache.spark" % "spark-core_2.11" % "2.2.0"

dependencyOverrides ++= Set(
  "io.netty" % "netty" % "3.9.9.Final",
  "commons-net" % "commons-net" % "2.2",
  "com.google.guava" % "guava" % "11.0.2"
)
