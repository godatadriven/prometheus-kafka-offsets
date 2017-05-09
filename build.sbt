import sbtassembly.AssemblyPlugin.autoImport._
import spray.revolver.RevolverPlugin.Revolver

organization := "com.godatadriven"
name := "prometheus-kafka-offsets"
version := "1.0"

scalaVersion := "2.10.6"
scalacOptions := Seq("-unchecked", "-deprecation", "-encoding", "utf8", "-target:jvm-1.7", "-feature")

incOptions    := incOptions.value.withNameHashing(nameHashing = true)
updateOptions := updateOptions.value.withCachedResolution(cachedResoluton = true)

val akkaV = "2.3.9"
val sprayV = "1.3.3"

resolvers += "spray repo" at "http://repo.spray.io"

libraryDependencies += "org.apache.kafka" %% "kafka" % "0.10.0.0"
libraryDependencies += "org.apache.kafka" % "kafka-clients" % "0.10.0.0"
libraryDependencies += "com.google.code.gson" % "gson" % "2.6.2"

libraryDependencies += "io.spray" %% "spray-json" % "1.3.1"
libraryDependencies += "io.spray" %% "spray-client" % sprayV
libraryDependencies += "io.spray" %% "spray-can" % sprayV
libraryDependencies += "io.spray" %% "spray-routing-shapeless2" % sprayV
libraryDependencies += "io.spray" %% "spray-httpx" % sprayV
libraryDependencies += "com.typesafe.akka" %% "akka-actor" % akkaV
libraryDependencies += "com.typesafe.akka" %% "akka-testkit" % akkaV % "test"
libraryDependencies += "org.specs2" %% "specs2-core" % "2.3.7" % "test"
libraryDependencies += "com.jsuereth" %% "scala-arm" % "1.4"


Revolver.settings

enablePlugins(JavaServerAppPackaging)

// Start of RPM config
maintainer in Rpm              := "Ron van Weverwijk <ronvanweverwijk@godatadriven.com>"
packageSummary in Rpm            := "REST service for collecting Kafka offset information for prometheus"
packageDescription               := "This package provides a REST service for Prometheus io."
startRunlevels in Rpm          := Some("2 3 4 5")
stopRunlevels in Rpm           := Some("0 1 6")
requiredStartFacilities in Rpm := Some("$local_fs $network $remote_fs")
requiredStopFacilities in Rpm  := Some("$local_fs $network $remote_fs")
rpmVendor                        := "com.godatadriven"
rpmLicense                       := Some("All rights reserved.")
rpmGroup                         := Some("com.godatadriven")
daemonShell in Linux             := "/bin/bash"
rpmBrpJavaRepackJars             := true

mappings in Universal <+= (packageBin in Compile, sourceDirectory ) map { (_, src) =>
  src / "main" / "resources" / "application.conf" -> "conf/app.conf"
}

bashScriptExtraDefines ++= Seq(
  """addJava "-Dconfig.file=${app_home}/../conf/app.conf"""",
  """addJava "-Djava.security.auth.login.config=/usr/share/prometheus-kafka-offsets/conf/prometheus-kafka-offsets-jaas.conf""""
)

bashScriptConfigLocation := Some(s"/etc/default/${name.value}")

defaultLinuxInstallLocation := "/usr/share"
// End of RPM config

// If running locally, ensure that "provided" dependencies are on the classpath.
run in Compile <<= Defaults.runTask(fullClasspath in Compile, mainClass in (Compile, run), runner in (Compile, run))

fullClasspath in Revolver.reStart ++= (fullClasspath in Compile).value

// Run things in a forked JVM, so we can set the options below.
fork in run := true

Keys.mainClass in (Compile) := Some("com.godatadriven.kafka.offset.Boot")
assemblyJarName in assembly := "prometheus-kafka-offsets_2.10-1.0.jar"