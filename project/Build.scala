import sbt._
import Keys._
import Tests._

object SqalaBuild extends Build {

lazy val sharedSettings = Seq(
  organizationName := "Active Group",
  organization := "de.ag.sqala",
  scalacOptions ++= List("-deprecation", "-feature", "-unchecked"),
  scalaVersion := "2.10.1",
  licenses += ("Two-clause BSD-style license", url("https://github.com/active-group/sqala/blob/master/LICENSE")),
  resolvers ++= Seq(
    "Sonatype Snapshots" at "http://oss.sonatype.org/content/repositories/snapshots",
    "Sonatype Releases" at "http://oss.sonatype.org/content/repositories/releases"
  )
)
/*lazy val aRootProject = Project(id = "root", base = file("."),
  settings = Project.defaultSettings ++ sharedSettings ++ Seq(
    sourceDirectory := file("target/root-src"),
    publishArtifact := false,
    test := (),
    testOnly <<= inputTask { argTask => (argTask) map { args => }}
  ))
  .aggregate(sqalaProject, sqalaTestProject)*/
lazy val sqalaProject = Project(id = "sqala", base = file("."),
  settings = Project.defaultSettings ++ sharedSettings ++ Seq(
    name := "sqala",
    description := "SQL bindings for Scala",
    test := (),
    testOnly <<= inputTask { argTask => (argTask) map { args => }}
  ))
lazy val sqalaTestProject = Project(id = "sqala-testkit", base = file("sqala-testkit"),
  settings = Project.defaultSettings ++ sharedSettings ++ Seq(
    name := "sqala-testkit",
    description := "Testing sqala",
    libraryDependencies ++= Seq(
        "org.scalatest" %% "scalatest" % "1.9.1",
        "org.xerial" % "sqlite-jdbc" % "3.7.2",
        "org.scalacheck" %% "scalacheck" % "1.10.1" % "test")
  )) dependsOn(sqalaProject)
}
                              

                      
