import sbtassembly.MergeStrategy

name := "aws-backend"

version := "1.0"

scalaVersion := "2.11.8"

val cromwellV = "25-f141ad1-SNAP"
val betterFilesV = "2.16.0"

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

resolvers ++= Seq(
  "Broad Artifactory Releases" at "https://artifactory.broadinstitute.org/artifactory/libs-release/"
)

/***
  * by default log buffering is set to true in sbt, which means
  * that for tests executed in parallel you will not see the
  * output until the test suite completes.  Setting this to false
  * will not buffer output, but it will be interleaved
  */
// logBuffered in Test := false

libraryDependencies ++= Seq(
  "org.broadinstitute" %% "cromwell-backend" % cromwellV % Provided,
  "com.github.pathikrit" %% "better-files" % betterFilesV,
  "com.amazonaws" % "aws-java-sdk" % "1.11.41",
  "com.github.kxbmap" %% "configs" % "0.4.2",
  "org.scalatest" %% "scalatest" % "2.2.6" % Test
)

scalacOptions ++= compilerSettings

val customMergeStrategy: String => MergeStrategy = {
  case x if Assembly.isConfigFile(x) =>
    MergeStrategy.concat
  case PathList(ps@_*) if Assembly.isReadme(ps.last) || Assembly.isLicenseFile(ps.last) =>
    MergeStrategy.rename
  case PathList("META-INF", path@_*) =>
    path map {
      _.toLowerCase
    } match {
      case ("manifest.mf" :: Nil) | ("index.list" :: Nil) | ("dependencies" :: Nil) =>
        MergeStrategy.discard
      case ps@(_ :: _) if ps.last.endsWith(".sf") || ps.last.endsWith(".dsa") =>
        MergeStrategy.discard
      case "plexus" :: _ =>
        MergeStrategy.discard
      case "spring.tooling" :: _ =>
        MergeStrategy.discard
      case "services" :: _ =>
        MergeStrategy.filterDistinctLines
      case ("spring.schemas" :: Nil) | ("spring.handlers" :: Nil) =>
        MergeStrategy.filterDistinctLines
      case _ => MergeStrategy.deduplicate
    }
  case "asm-license.txt" | "overview.html" | "cobertura.properties" =>
    MergeStrategy.discard
  case _ => MergeStrategy.deduplicate
}

assemblyMergeStrategy in assembly := customMergeStrategy
assemblyJarName in assembly := name.value + "-" + version.value + ".jar"
assemblyOption in assembly := (assemblyOption in assembly).value.copy(includeScala = false)
