val scala3Version = "3.4.2"

lazy val root = project
  .in(file("."))
  .settings(
    name := "onebrc",
    version := "0.1.0-SNAPSHOT",

    scalaVersion := scala3Version,

    libraryDependencies += "org.scalameta" %% "munit" % "0.7.29" % Test,

    fork := true,
  )

enablePlugins(JmhPlugin)

enablePlugins(JavaAppPackaging)
enablePlugins(GraalVMNativeImagePlugin)
graalVMNativeImageCommand := "/home/bertrand/.sdkman/candidates/java/22.0.1-graal/bin/native-image"
graalVMNativeImageOptions := Seq(
  "--no-fallback",
  // "--gc=G1",
  "--gc=epsilon",
  // "--pgo-instrument",
  "--pgo=/home/bertrand/Code/onebrc/default.iprof",
  "-march=native",
  "-R:MaxHeapSize=16g",
  // "-R:PercentTimeInIncrementalCollection=25"
)

Compile / scalacOptions += "-language:strictEquality"
