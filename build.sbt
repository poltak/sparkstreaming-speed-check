name := "SpeedCheck"

version := "1.0"

scalaVersion := "2.10.3"

libraryDependencies += "org.apache.spark" %% "spark-streaming" % "1.3.0"

resourceDirectory in Compile := baseDirectory.value / "resources"
