import sbt._
import sbt.Keys._
import com.typesafe.sbt.SbtAspectj.{ Aspectj, aspectjSettings, useInstrumentedClasses }
import com.typesafe.sbt.SbtAspectj.AspectjKeys.inputs

object STS2Application extends Build {
  val debugInUse = SettingKey[Boolean]("debug-in-use", "debug is used")

  lazy val sets2app = Project(
    id = "randomSearch",
    base = file("."),
    settings = Defaults.defaultSettings ++ aspectjSettings ++ Seq(
      organization := "com.typesafe.sbt.aspectj",
      version := "0.1",
      scalaVersion := "2.11.2",
      libraryDependencies += "com.typesafe.akka" %% "akka-actor" % "2.3.6",
      libraryDependencies += "com.typesafe.akka" %% "akka-cluster" % "2.3.6",
      libraryDependencies += "com.assembla.scala-incubator" %% "graph-core" % "1.9.0",
      libraryDependencies += "com.assembla.scala-incubator" %% "graph-dot" % "1.9.0",
      //libraryDependencies += "com.typesafe.scala-logging" %% "scala-logging-api" % "2.1.2",
      libraryDependencies += "ch.qos.logback" % "logback-classic" % "1.1.2",
      // For 2.11.0:
      libraryDependencies += "org.scala-lang.modules" %% "scala-parser-combinators" % "1.0.2",
      // For 2.10.*:
      //libraryDependencies += "org.scala-lang" % "scala-swing" % scalaVersion.value,

      // Also add scala-library, for Await.result and Await.ready
      inputs in Aspectj <++= update map { report =>
        report.matching(moduleFilter(organization = "org.scala-lang", name = "scala-library*"))
      },

      // // add akka-actor as an aspectj input (find it in the update report)
      // inputs in Aspectj <++= update map { report =>
      //   report.matching(moduleFilter(organization = "com.typesafe.akka", name = "akka-actor*"))
      // },

      // replace the original akka-actor and scala-library jars with the instrumented classes in runtime
      fullClasspath in Runtime <<= useInstrumentedClasses(Runtime)
    )
  )
}
