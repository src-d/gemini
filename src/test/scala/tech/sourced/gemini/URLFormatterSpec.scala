package tech.sourced.gemini

import org.scalatest.{FlatSpec, Matchers}

class URLFormatterSpec extends FlatSpec
  with Matchers {

  "URLFormatter" should "format correctly" in {
    case class Input(repo: String, commit: String, path: String)

    val cases = Map(
      Input("github.com/src-d/test", "sha1", "path/file") -> "https://github.com/src-d/test/blob/sha1/path/file",
      Input("gitlab.com/src-d/test", "sha1", "path/file") -> "https://gitlab.com/src-d/test/blob/sha1/path/file",
      Input("bitbucket.org/src-d/test", "sha1", "path/file") -> "https://bitbucket.org/src-d/test/src/sha1/path/file",
      Input("github.com/src-d/test.git", "sha1", "path/file") -> "https://github.com/src-d/test/blob/sha1/path/file",
      Input("unknown", "sha1", "path/file") -> "repo: unknown commit: sha1 path: path/file"
    )

    for ((i, expected) <- cases) {
      URLFormatter.format(i.repo, i.commit, i.path) should be(expected)
    }
  }
}
