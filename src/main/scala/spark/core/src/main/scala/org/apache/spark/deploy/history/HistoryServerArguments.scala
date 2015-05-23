/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.deploy.history

import org.apache.spark.SparkConf
import org.apache.spark.util.Utils

/**
 * Command-line parser for the master.
 */
private[spark] class HistoryServerArguments(conf: SparkConf, args: Array[String]) {
  private var logDir: String = null

  parse(args.toList)

  private def parse(args: List[String]): Unit = {
    args match {
      case ("--dir" | "-d") :: value :: tail =>
        logDir = value
        parse(tail)

      case ("--help" | "-h") :: tail =>
        printUsageAndExit(0)

      case Nil =>

      case _ =>
        printUsageAndExit(1)
    }
    if (logDir != null) {
      conf.set("spark.history.fs.logDirectory", logDir)
    }
  }

  private def printUsageAndExit(exitCode: Int) {
    System.err.println(
      """
      |Usage: HistoryServer
      |
      |Configuration options can be set by setting the corresponding JVM system property.
      |History Server options are always available; additional options depend on the provider.
      |
      |History Server options:
      |
      |  spark.history.ui.port              Port where server will listen for connections
      |                                     (default 18080)
      |  spark.history.acls.enable          Whether to enable view acls for all applications
      |                                     (default false)
      |  spark.history.provider             Name of history provider class (defaults to
      |                                     file system-based provider)
      |  spark.history.retainedApplications Max number of application UIs to keep loaded in memory
      |                                     (default 50)
      |FsHistoryProvider options:
      |
      |  spark.history.fs.logDirectory      Directory where app logs are stored (required)
      |  spark.history.fs.updateInterval    How often to reload log data from storage (in seconds,
      |                                     default 10)
      |""".stripMargin)
    System.exit(exitCode)
  }

}
