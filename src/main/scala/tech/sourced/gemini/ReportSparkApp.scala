package tech.sourced.gemini

object ReportSparkApp extends App {
  val reposPath = args(1)
  println(s"Reporting all duplicates of: $reposPath") // "in (project1, project2, ...)"?

  //for every file
  // query Cassandry by hash
  // .collect()
  //print

  // Output:
  //
  // Project1
  //  file1:sha1 - Project1:sha1, Project2:sha1, Project3:sha3 ...
  //  file2:sha2 - Project1:sha2, Project2:sha2, ...
  //  .
  //  .
  //  fileN:shaN - ProjectN:shaN
}
