import sbt._
import Keys._


object SqalaBuild extends Build {
  lazy val root =
    Project(id = "Sqala",
      base = file("."),
      settings = Defaults.coreDefaultSettings ++ Seq(
        scalaVersion := "2.10.6",
        crossScalaVersions := Seq("2.10.6", "2.12.2"),
        libraryDependencies ++= Seq(
          // needed for: minitest
          "io.monix" %% "minitest" % "1.1.0" % "test"),
        testFrameworks += new TestFramework("minitest.runner.Framework")
      ))
}



