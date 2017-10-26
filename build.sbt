lazy val root =
  (project in file("."))
    .settings(
      scalaVersion := "2.10.6",
      crossScalaVersions := Seq("2.10.6", "2.11.8", "2.12.2"),
      libraryDependencies ++= Seq(
        // needed for: minitest
        "io.monix" %% "minitest" % "1.1.0" % "test"),
      testFrameworks += new TestFramework("minitest.runner.Framework")
    )
