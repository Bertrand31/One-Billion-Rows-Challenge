val scala3Version = "3.4.2"

lazy val root = project
  .in(file("."))
  .settings(
    name := "onebrc",
    version := "0.1.0-SNAPSHOT",

    scalaVersion := scala3Version,

    libraryDependencies += "org.scalameta" %% "munit" % "1.0.0" % Test,

    fork := true,
  )

enablePlugins(JmhPlugin)

enablePlugins(JavaAppPackaging)
enablePlugins(GraalVMNativeImagePlugin)
graalVMNativeImageCommand := "/home/bertrand/.asdf/installs/java/oracle-graalvm-22/lib/svm/bin/native-image"
graalVMNativeImageOptions := Seq(
  "--no-fallback",
  // "--gc=G1",
  // "--gc=epsilon",
  // "--pgo-instrument",
  "--pgo=/home/bertrand/Code/onebrc/default.iprof",
  "-march=native",
  "-R:PercentTimeInIncrementalCollection=80",
  "-R:MaxHeapSize=25g",
  // "-R:MaxNewSize=1g",
)

Compile / scalacOptions += "-language:strictEquality"
