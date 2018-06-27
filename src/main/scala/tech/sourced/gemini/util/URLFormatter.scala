package tech.sourced.gemini.util

object URLFormatter {
  private val services = Map(
    "github.com" -> "https://%s/blob/%s/%s",
    "bitbucket.org" -> "https://%s/src/%s/%s",
    "gitlab.com" -> "https://%s/blob/%s/%s"
  )
  private val default = ("", "repo: %s commit: %s path: %s")

  def format(repo: String, commit: String, path: String): String = {
    val urlTemplateByRepo = services.find { case (h, _) => repo.startsWith(h) }.getOrElse(default)._2
    val repoWithoutSuffix = repo.replaceFirst("\\.git$", "")

    urlTemplateByRepo.format(repoWithoutSuffix, commit, path)
  }
}
