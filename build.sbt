lazy val root =
  (project in file("."))
    .settings(
      name := "Sqala",
      version := "0.2.10-SNAPSHOT",
      scalaVersion := "2.10.6",
      crossScalaVersions := Seq("2.10.6", "2.11.8", "2.12.2"),
      libraryDependencies ++= Seq(
        "org.scalatest" %% "scalatest" % "3.0.4" % "test"
      ),
      testOptions in Test += Tests.Argument("-oF")
    )
