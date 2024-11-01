val sparkVersion = "3.3.2"
val scala2Version = "2.13.11"

lazy val root = project
  .in(file("."))
  .settings(
    name := "product-order",
    version := "0.1",
    scalaVersion := scala2Version,
    Compile / unmanagedSourceDirectories += baseDirectory.value / "DAOs" ,
    Compile / unmanagedSourceDirectories += baseDirectory.value / "src" ,

    libraryDependencies ++= Seq(
    "org.apache.spark" %% "spark-core" % sparkVersion % "provided",
    "org.apache.spark" %% "spark-sql" % sparkVersion % "provided",
    "io.delta" %% "delta-core" % "2.3.0"
    )
  )

assembly / assemblyMergeStrategy := {
 case PathList("META-INF", _*) => MergeStrategy.discard
 case _                        => MergeStrategy.first
}