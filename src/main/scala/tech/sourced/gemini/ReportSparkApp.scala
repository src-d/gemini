package tech.sourced.gemini

object ReportSparkApp extends App {
  def printUsage(): Unit = {
    println("Usage: ./report <repository>")
    println("")
    println("Finds duplicated files among hashed repositories")
    println("  <repository> - repository url, example: github.com/src-d/go-git.git")
    System.exit(2)
  }

  if (args.length <= 0) {
    printUsage()
  }

  val repository = args(0)
  println(s"Reporting all duplicates of: $repository")

  val similar = Gemini.report(repository)

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
