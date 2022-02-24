name := "mlproject"

version := "1.0"

scalaVersion := "2.11.12"


libraryDependencies ++= Seq(
  "org.apache.spark"      %% "spark-core"                  % "2.4.7" % Provided,
  "org.apache.spark"      %% "spark-sql"                   % "2.4.7" % Provided,
  "org.apache.spark"      %% "spark-hive"                  % "2.4.7" % Provided,
  "org.apache.spark"      %% "spark-mllib"                 % "2.4.6" % Provided
)

assemblyJarName in assembly := s"${name.value}-${version.value}.jar"
test in assembly := {}

assemblyMergeStrategy in assembly := {
  case PathList("javax", "servlet", xs @ _*)         => MergeStrategy.first
  case PathList(ps @ _*) if ps.last endsWith ".html" => MergeStrategy.first
  case "application.conf"                            => MergeStrategy.concat
  case x =>
    val oldStrategy = (assemblyMergeStrategy in assembly).value
    oldStrategy(x)
}