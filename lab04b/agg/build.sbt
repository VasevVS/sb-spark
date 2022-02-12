name := "agg"

version := "1.0"

scalaVersion := "2.11.12"

val sparkVersion = "2.4.7"

libraryDependencies += "org.apache.spark" %% "spark-core" % sparkVersion % Provided
libraryDependencies += "org.apache.spark" %% "spark-sql"  % sparkVersion % Provided

scalacOptions ++= Seq(
  "-encoding",
  "UTF-8",
  "-feature",
  "-unchecked",
  "-language:existentials",
  "-language:higherKinds",
  "-Ywarn-dead-code",
  "-Ywarn-numeric-widen"
)

assemblyJarName in assembly := s"${name.value}-${version.value}.jar"