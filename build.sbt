

ThisBuild / version := "0.1.0-SNAPSHOT"

ThisBuild / scalaVersion := "2.13.8"

lazy val root = (project in file("."))
  .settings(
    name := "sequence_file_fs2",
    libraryDependencies ++= Seq(
      "co.fs2" %% "fs2-core" % "3.2.5",
      "co.fs2" %% "fs2-io" % "3.2.5",
      "org.apache.hadoop" % "hadoop-client" % "3.3.1",
      "org.apache.hadoop" % "hadoop-common" % "3.3.1",

      "org.typelevel" %% "munit-cats-effect-3" % "1.0.7" % Test,
    )
  )
