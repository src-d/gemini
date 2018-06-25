package tech.sourced.gemini.util

import org.apache.log4j.{Level, LogManager}
import org.slf4j.{LoggerFactory, Logger => Slf4jLogger}

object Logger {
  def apply(name: String, verbose: Boolean = false): Slf4jLogger = {
    if (verbose) {
      LogManager.getRootLogger.setLevel(Level.INFO)
    }
    LoggerFactory.getLogger(name)
  }
}
