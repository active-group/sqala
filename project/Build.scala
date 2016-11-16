import sbt._
import Keys._


object SqalaBuild extends Build {
  lazy val root =
    Project(id = "Sqala",
      base = file("."),
      settings = Defaults.coreDefaultSettings ++ Seq(
        scalaVersion := "2.10.6",
        libraryDependencies ++= Seq(
          // needed for: minitest
          "io.monix" %% "minitest" % "0.19" % "test"),
        testFrameworks += new TestFramework("minitest.runner.Framework")
      ))
}



