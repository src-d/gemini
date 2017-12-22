package tech.sourced.gemini

import org.scalatest.{FlatSpec, Matchers}

class URLFormatterSpec extends FlatSpec
  with Matchers {

  "URLFormatter" should "format correctly" in {
    case class Input(repo: String, ref_hash: String, file: String)

    val cases = Map(
      Input("github.com/src-d/test", "sha1", "path/file") -> "https://github.com/src-d/test/blob/sha1/path/file",
      Input("gitlab.com/src-d/test", "sha1", "path/file") -> "https://gitlab.com/src-d/test/blob/sha1/path/file",
      Input("bitbucket.org/src-d/test", "sha1", "path/file") -> "https://bitbucket.org/src-d/test/src/sha1/path/file",
      Input("github.com/src-d/test.git", "sha1", "path/file") -> "https://github.com/src-d/test/blob/sha1/path/file",
      Input("unknown", "sha1", "path/file") -> "repo: unknown ref_hash: sha1 file: path/file"
    )

    for ((i, expected) <- cases) URLFormatter.format(i.repo, i.ref_hash, i.file) should be(expected)
  }
}
