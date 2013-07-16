name := "sqala"

scalaVersion := "2.10.1"

scalacOptions += "-deprecation"

scalacOptions += "-unchecked"

scalacOptions += "-feature"

// libraryDependencies += "org.specs2" %% "specs2" % "1.14" % "test"

// libraryDependencies += "org.codehaus.groovy" % "groovy" % "2.1.5"

// libraryDependencies += "com.github.nscala-time" %% "nscala-time" % "0.4.2"

// sqlite jdbc bindings; as of 2013-07-16, current sqlite is 3.7.17,
// but org.xerial has only 3.7.15-M1
libraryDependencies += "org.xerial" % "sqlite-jdbc" % "3.7.2"

// scalatest
// as of 2013-07-16, only milestones for 2.0
libraryDependencies += "org.scalatest" %% "scalatest" % "1.9.1"

resolvers ++= Seq(
  "Sonatype Snapshots" at "http://oss.sonatype.org/content/repositories/snapshots",
  "Sonatype Releases" at "http://oss.sonatype.org/content/repositories/releases"
)

libraryDependencies ++= Seq(
  "org.scalacheck" %% "scalacheck" % "1.10.1" % "test"
)

TaskKey[Unit]("print-run-classpath") <<= (fullClasspath in Runtime) map { cp => println(cp.files.absString)}
