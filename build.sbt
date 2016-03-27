name := "prometheus-kafka-offsets"

version := "1.0"

scalaVersion := "2.10.6"

val sprayV = "1.3.3"
val akkaV = "2.3.9"

libraryDependencies += "org.apache.kafka" %% "kafka" % "0.9.0.2.3.4.0-3485"
libraryDependencies += "org.apache.kafka" % "kafka-clients" % "0.9.0.2.3.4.0-3485"
libraryDependencies += "com.google.code.gson" % "gson" % "2.6.2"

libraryDependencies += "io.spray" %% "spray-json" % "1.3.1"
libraryDependencies += "io.spray" %% "spray-client" % sprayV
libraryDependencies += "io.spray" %% "spray-can" % sprayV
libraryDependencies += "io.spray" %% "spray-routing-shapeless2" % sprayV
libraryDependencies += "io.spray" %% "spray-httpx" % sprayV
libraryDependencies += "com.typesafe.akka" %% "akka-actor" % akkaV
libraryDependencies += "com.typesafe.akka" %% "akka-testkit" % akkaV % "test"
libraryDependencies += "org.specs2" %% "specs2-core" % "2.3.7" % "test"
