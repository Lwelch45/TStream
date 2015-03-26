lazy val root = (project in file(".")).
  settings(
    name := "TStream",
    version := "1.0",
    scalaVersion := "2.11.4",
    mainClass in Compile := Some("com.laurence.tstream.TStream")        
)


libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % "1.3.0" % "provided",
  "org.apache.spark" %% "spark-streaming" % "1.3.0" % "provided",
  "org.apache.spark" % "spark-streaming-twitter_2.10" % "1.3.0",
  "org.apache.httpcomponents" % "httpclient" % "4.4",
  "org.apache.httpcomponents" % "httpcore" % "4.4.1",  
  "edu.stanford.nlp" % "stanford-corenlp" % "3.5.1"
)

// META-INF discarding
mergeStrategy in assembly <<= (mergeStrategy in assembly) { (old) =>
{
  case PathList("META-INF", xs @ _*) => MergeStrategy.discard
  case x => MergeStrategy.first
}
}


