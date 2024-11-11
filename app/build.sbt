val scala3Version = "2.12.16"

lazy val root = project
  .in(file("."))
  .settings(
    name := "app",
    version := "0.1.0-SNAPSHOT",

    scalaVersion := scala3Version,

libraryDependencies ++= Seq( 
  "org.apache.spark" %% "spark-core" % "3.5.0",
 "org.apache.spark" %% "spark-sql" % "3.5.0",
 "org.scalameta" %% "munit" % "1.0.0" % Test
)

  )