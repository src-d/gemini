package tech.sourced.gemini

import org.apache.log4j.{Level, LogManager}
import org.slf4j.LoggerFactory
import org.slf4j.{Logger => Slf4jLogger}

object Logger {
  def apply(name: String, verbose: Boolean = false): Slf4jLogger = {
    if (verbose) {
      LogManager.getRootLogger().setLevel(Level.INFO)
    }
    LoggerFactory.getLogger(name)
  }
}
