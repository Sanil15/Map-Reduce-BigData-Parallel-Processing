lazy val root = (project in file(".")).
  settings(
    name := "com.mapreduce.main.Driver",
    version := "1.0",
    mainClass in Compile := Some("com.mapreduce.main.Driver")
  )

// Scala Runtime
libraryDependencies += "org.apache.spark" %% "spark-core" % "1.5.2"
libraryDependencies += "org.apache.spark" % "spark-streaming_2.10" % "1.6.1"
libraryDependencies += "org.apache.spark" % "spark-sql_2.10" % "2.0.1"



