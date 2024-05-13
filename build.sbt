version := "1.0.0"
scalaVersion := "2.12.15"

name := "FlightAnalyzer"

lazy val sparkVersion = "3.2.1"

lazy val sparkDependencies = Seq(
	"org.apache.spark" %% "spark-core",
	"org.apache.spark" %% "spark-sql"
).map(_ % sparkVersion % Provided)

lazy val otherDependencies = Seq(
	"com.github.pureconfig" %% "pureconfig" % "0.14.0",
	"com.github.scopt" %% "scopt" % "3.7.0"
)

libraryDependencies ++= (sparkDependencies ++ otherDependencies)

mainClass in assembly := Some("com.example.FlightAnalyzer")

assemblyJarName in assembly := f"${name.value}.${version.value}.jar"

assemblyOption in assembly := (assemblyOption in assembly).value.copy(includeScala = false)

test in assembly := {}

assemblyMergeStrategy in assembly := {
	case PathList("META-INF", xs@_*) => MergeStrategy.discard
	case x => MergeStrategy.first
}