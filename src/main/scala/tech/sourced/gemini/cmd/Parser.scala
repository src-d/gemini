package tech.sourced.gemini.cmd

abstract class Parser[C](programName: String) extends scopt.OptionParser[C](programName) {
  /**
    * parses the given `args` but reads environment variables also
    */
  def parseWithEnv(args: Seq[String], init: C): Option[C] = {
    val shortToFullMapping = options
      .filter(opt => opt.shortOpt.isDefined)
      .foldLeft(Map[String, String]()) { case (opts, opt) =>
        opts + ("-" + opt.shortOpt.get -> opt.fullName)
      }

    val cliNames = args
      .filter(arg => arg.startsWith("-")) // remove positional arguments
      .map(arg => arg.split("=", 2)(0))
      .map(name => shortToFullMapping.getOrElse(name, name)) // convert short names to full names

    val envNames = options
      .map(opt => opt.fullName)
      .filter(name => name.startsWith("--")) // remove positional arguments
      .filterNot(name => name == "verbose") // we don't support verbose as env var
      .filterNot(name => cliNames.contains(name)) // don't read env vars for passed flags
      .map(name => name.stripPrefix("--"))
      .map(name => name.replaceAll("-", "_"))
      .map(name => name.toUpperCase)

    val envValues = envNames.foldLeft(Map[String, String]()) { case (opts, name) =>
      sys.env.get(name) match {
        case Some(value) => opts + (name -> value)
        case None => opts
      }
    }

    val envArgs = envValues.foldLeft(Seq[String]()) { case (opts, (name, value)) =>
      // transform name back to cli param
      val cliName = name.replaceAll("_", "-").toLowerCase
      opts :+ s"--$cliName=$value"
    }

    parse(envArgs++args, init)
  }
}
