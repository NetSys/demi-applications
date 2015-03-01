import sbt._
import sbt.Keys._
import com.typesafe.sbt.SbtAspectj.{ Aspectj, aspectjSettings, useInstrumentedClasses }
import com.typesafe.sbt.SbtAspectj.AspectjKeys.inputs

object STS2Application extends Build {
  val appName = "akka-raft"
  val appVersion = "1.0-SNAPSHOT"

  // import Dependencies._

  val debugInUse = SettingKey[Boolean]("debug-in-use", "debug is used")

  // TODO(cs): need to make 2.3.8?
  val akkaVersion = "2.3.6"

  lazy val sets2app = Project(
    id = "randomSearch",
    base = file("."),
    settings = Defaults.defaultSettings ++ aspectjSettings ++ Seq(
      organization := "com.typesafe.sbt.aspectj",
      version := "0.1",
      scalaVersion := "2.11.0",
      libraryDependencies += "com.typesafe.akka" %% "akka-actor" % akkaVersion,
      libraryDependencies += "com.typesafe.akka" %% "akka-cluster" % akkaVersion,
      libraryDependencies += "com.typesafe.akka" %% "akka-slf4j"   % akkaVersion,
      libraryDependencies += "com.typesafe.akka" %% "akka-persistence-experimental" % akkaVersion,
      libraryDependencies += "com.typesafe.akka" %% "akka-testkit" % akkaVersion % "test",
      libraryDependencies += "com.typesafe.akka" %% "akka-multi-node-testkit" % akkaVersion % "test",
      libraryDependencies += "org.mockito"        % "mockito-core"   % "1.9.5"     % "test",
      libraryDependencies += "org.scalatest"     %% "scalatest"      % "2.2.1"     % "test",

      libraryDependencies += "org.scala-lang.modules" %% "scala-parser-combinators" % "1.0.2",
      libraryDependencies += "com.assembla.scala-incubator" %% "graph-core" % "1.9.0",
      libraryDependencies += "com.assembla.scala-incubator" %% "graph-dot" % "1.9.0",
      libraryDependencies += "com.typesafe.scala-logging" %% "scala-logging" % "3.1.0",
      libraryDependencies += "ch.qos.logback" % "logback-classic" % "1.1.2",

      // add akka-actor as an aspectj input (find it in the update report)
      inputs in Aspectj <++= update map { report =>
        report.matching(moduleFilter(organization = "com.typesafe.akka", name = "akka-actor*"))
      },

      // replace the original akka-actor jar with the instrumented classes in runtime
      fullClasspath in Runtime <<= useInstrumentedClasses(Runtime)
    )
  )// .settings(
  //   libraryDependencies ++= generalDependencies
  //)
}
