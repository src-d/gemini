import Dependencies.{scalaTest, _}
import sbt.Keys.libraryDependencies

organization := "tech.sourced"
scalaVersion := "2.11.11"
version := "0.0.2-SNAPSHOT"

name := "gemini"

libraryDependencies ++= Seq(
  scalaTest % Test,
  scoverage % Test,
  spark % Test,

  sparkSql % Provided,
  fixNewerHadoopClient % Provided, //due to newer v. of guava

  scalaLib % Compile,
  scalapb % Compile,
  ioGrpc % Compile,
  scalapbGrpc % Compile,
  engine % Compile,
  jgit % Compile,
  fixNetty,
  cassandraDriverMetrics % Provided, //needed for using Driver \wo Spark from SparkConnector
  cassandraSparkConnector % Compile,
  scopt % Compile,
  slf4jApi % Compile,
  log4j12 % Compile,
  log4jBinding % Compile,
  commonsMath % Compile,
  bblfshClient % Compile,
  avro % Compile,
  parquetAvro % Compile,
  hadoopCommon % Compile
)
assemblyOption in assembly := (assemblyOption in assembly).value.copy(includeScala = false, includeDependency = false)
assemblyJarName in assembly := s"${name.value}-uber.jar"

assemblyMergeStrategy in assembly := {
  case "META-INF/io.netty.versions.properties" => MergeStrategy.last
  case x =>
    val oldStrategy = (assemblyMergeStrategy in assembly).value
    oldStrategy(x)
}

assemblyShadeRules in assembly := Seq(
  ShadeRule.rename("io.netty.**" -> "io.shadednetty.@1").inAll,
  // bblfsh/scalapb use newer versions than spark
  ShadeRule.rename("com.google.common.**" -> "shaded.com.google.common.@1").inAll,
  ShadeRule.rename("com.google.protobuf.**" -> "shaded.com.google.protobuf.@1").inAll,
  // bblfsh in engine is old
  ShadeRule.rename("com.trueaccord.scalapb.**" -> "scalapb.@1").inAll
)

test in assembly := {}
testOptions in Test += Tests.Argument(TestFrameworks.ScalaTest, "-oUT")
testOptions in Test += Tests.Argument(TestFrameworks.ScalaTest, "-l", "Cassandra")

parallelExecution in Test := false
fork in Test := true //Forking is required for the Embedded Cassandra
logBuffered in Test := false

showSuccess := false
sonatypeProfileName := "tech.sourced"

// pom settings for sonatype
homepage := Some(url("https://github.com/src-d/gemini"))
scmInfo := Some(ScmInfo(url("https://github.com/src-d/gemini"),
  "git@github.com:src-d/gemini.git"))
developers += Developer("bzz",
  "Alexander Bezzubov",
  "alex@sourced.tech",
  url("https://github.com/bzz"))
licenses += ("GPLv3", url("https://www.gnu.org/licenses/gpl.html"))
pomIncludeRepository := (_ => false)

crossPaths := false
publishMavenStyle := true

val SONATYPE_USERNAME = scala.util.Properties.envOrElse("SONATYPE_USERNAME", "NOT_SET")
val SONATYPE_PASSWORD = scala.util.Properties.envOrElse("SONATYPE_PASSWORD", "NOT_SET")
credentials += Credentials(
  "Sonatype Nexus Repository Manager",
  "oss.sonatype.org",
  SONATYPE_USERNAME,
  SONATYPE_PASSWORD)

val SONATYPE_PASSPHRASE = scala.util.Properties.envOrElse("SONATYPE_PASSPHRASE", "not set")

useGpg := false
pgpSecretRing := baseDirectory.value / "project" / ".gnupg" / "secring.gpg"
pgpPublicRing := baseDirectory.value / "project" / ".gnupg" / "pubring.gpg"
pgpPassphrase := Some(SONATYPE_PASSPHRASE.toArray)

isSnapshot := version.value endsWith "SNAPSHOT"

publishTo := {
  val nexus = "https://oss.sonatype.org/"
  if (isSnapshot.value) {
    Some("snapshots" at nexus + "content/repositories/snapshots")
  } else {
    Some("releases" at nexus + "service/local/staging/deploy/maven2")
  }
}

scalastyleSources in Compile := {
  // all .scala files in "src/main/scala"
  val scalaSourceFiles = ((scalaSource in Compile).value ** "*.scala").get
  val fSep = java.io.File.separator
  val dirsNameToExclude = List(
    "com" + fSep + "google",
    "tech" + fSep + "sourced" + fSep + "featurext" + fSep + "generated",
    "gopkg" + fSep + "in"
  )
  scalaSourceFiles.filterNot(f => dirsNameToExclude.exists(dir => f.getAbsolutePath.contains(dir)))
}
