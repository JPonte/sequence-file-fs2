enablePlugins(GitVersioning)

git.gitTagToVersionNumber := { tag: String =>
  if (tag matches "[0-9]+\\..*") Some(tag)
  else None
}
git.useGitDescribe := true

name := "sequence_file_fs2"
organization := "org.jponte"
crossScalaVersions := Seq("2.12.15", "2.13.8", "3.1.1")
githubOwner := "JPonte"
githubRepository := "sequence-file-fs2"
githubTokenSource := TokenSource.GitConfig("github.token")

libraryDependencies ++= Seq(
  "co.fs2" %% "fs2-core" % "3.2.5",
  "co.fs2" %% "fs2-io" % "3.2.5",
  "org.apache.hadoop" % "hadoop-client" % "3.3.1",
  "org.apache.hadoop" % "hadoop-common" % "3.3.1",
  "org.typelevel" %% "munit-cats-effect-3" % "1.0.7" % Test,
)

scalacOptions ++= Seq(
  "-feature",
  "-deprecation",
  "-unchecked",
  "-language:higherKinds"
)
