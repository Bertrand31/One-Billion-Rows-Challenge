val scala3Version = "3.4.0"

lazy val root = project
  .in(file("."))
  .settings(
    name := "onebrc",
    version := "0.1.0-SNAPSHOT",

    scalaVersion := scala3Version,

    libraryDependencies += "org.typelevel" %% "cats-core" % "2.10.0",
    libraryDependencies += "org.typelevel" %% "cats-effect" % "3.5.4",
    libraryDependencies += "co.fs2" %% "fs2-core" % "3.9.4",
    libraryDependencies += "co.fs2" %% "fs2-io" % "3.9.4",
    libraryDependencies += "org.scalameta" %% "munit" % "0.7.29" % Test,

    fork := true,
    javaOptions ++= Seq(
      "-Xverify:none"
    )
  )
