// Author: Nat Tuck

lazy val root = (project in file(".")).
  settings(
    name := "Demo",
    version := "1.0",
    mainClass in Compile := Some("main.Demo")
  )

// Scala Runtime
libraryDependencies += "org.scala-lang" % "scala-library" % scalaVersion.value

// Hadoop
libraryDependencies += "org.apache.hadoop" % "hadoop-common" % "2.6.0"
libraryDependencies += "org.apache.hadoop" % "hadoop-mapreduce" % "2.6.0"
libraryDependencies += "org.apache.hadoop" % "hadoop-mapreduce-client-common" % "2.6.0"

