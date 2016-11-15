
name := "aws-backend"

version := "1.0"

scalaVersion := "2.11.8"

val akkaV = "2.3.14"

val compilerSettings = List(
  "-Xlint",
  "-feature",
  "-Xmax-classfile-name", "200",
  "-target:jvm-1.8",
  "-encoding", "UTF-8",
  "-unchecked",
  "-deprecation",
  "-Xfuture",
  "-Yno-adapted-args",
  "-Ywarn-dead-code",
  "-Ywarn-numeric-widen",
  "-Ywarn-value-discard",
  "-Ywarn-unused",
  "-Ywarn-unused-import",
  "-Xfatal-warnings"
)

/***
 * by default log buffering is set to true in sbt, which means
 * that for tests executed in parallel you will not see the 
 * output until the test suite completes.  Setting this to false
 * will not buffer output, but it will be interleaved
 */
// logBuffered in Test := false

libraryDependencies ++= Seq(
  "com.amazonaws" % "aws-java-sdk" % "1.11.41",
  "com.github.kxbmap" %% "configs" % "0.4.2",
  "com.typesafe" % "config" % "1.3.0",
  "org.typelevel" %% "cats" % "0.7.2",
  "com.typesafe.akka" %% "akka-actor" % akkaV,
  "org.scalatest" %% "scalatest" % "2.2.6" % Test,
  "com.github.pathikrit" %% "better-files" % "2.13.0"
)

scalacOptions ++= compilerSettings