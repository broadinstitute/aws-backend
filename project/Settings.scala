import Merging.customMergeStrategy
import sbt.Keys._
import sbt._
import sbtassembly.AssemblyPlugin.autoImport._

object Settings {
  lazy val assemblySettings = Seq(
    assemblyJarName in assembly := name.value + "-" + version.value + ".jar",
    aggregate in assembly := false,
    test in assembly := {},
    logLevel in assembly := Level.Info,
    assemblyMergeStrategy in assembly := customMergeStrategy
  )
}
