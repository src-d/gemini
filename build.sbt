import Dependencies.{scalaTest, _}
import sbt.Keys.libraryDependencies

organization := "tech.sourced"
scalaVersion := "2.11.11"
version := "0.0.1-SNAPSHOT"

name := "gemini"
libraryDependencies ++= Seq(
  scalaTest % Test,
  scoverage % Test,
  cassandra % Test,
  cassandraSparkConnectorEmbedded % Test exclude("com.datastax.cassandra", "cassandra-driver-core"),
  spark % Test,

  sparkSql % Provided,
  scalaLib % Compile,
  engine % Compile,
  jgit % Compile,
  fixNetty,
  cassandraSparkConnector % Compile exclude("com.datastax.cassandra", "cassandra-driver-core"),
  cassandraDriver % Compile exclude("io.netty", "*")
).map(_.exclude("org.slf4j", "log4j-over-slf4j"))  // Cassandra embedded does not run with those

assemblyOption in assembly := (assemblyOption in assembly).value.copy(includeScala = true)
assemblyJarName in assembly := s"${name.value}-uber.jar"

assemblyMergeStrategy in assembly := {
  case "META-INF/io.netty.versions.properties" => MergeStrategy.last
  case x =>
    val oldStrategy = (assemblyMergeStrategy in assembly).value
    oldStrategy(x)
}

assemblyShadeRules in assembly := Seq(
  ShadeRule.rename("com.google.common.**" -> "com.google.shadedcommon.@1").inAll,
  ShadeRule.rename("io.netty.**" -> "io.shadednetty.@1").inAll
)

test in assembly := {}
testOptions in Test += Tests.Argument(TestFrameworks.ScalaTest, "-oUT")
parallelExecution in Test := false
fork in Test := true //Forking is required for the Embedded Cassandra
logBuffered in Test := false

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
