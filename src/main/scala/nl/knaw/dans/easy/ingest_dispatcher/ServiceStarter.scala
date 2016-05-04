/**
 * Copyright (C) 2015-2016 DANS - Data Archiving and Networked Services (info@dans.knaw.nl)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package nl.knaw.dans.easy.ingest_dispatcher

import java.io.File
import java.util.concurrent.TimeUnit

import org.apache.commons.daemon.{Daemon, DaemonContext}
import org.slf4j.LoggerFactory

import scala.concurrent.duration.Duration

class ServiceStarter extends Daemon {
  val log = LoggerFactory.getLogger(getClass)

  implicit val s = Settings(
    depositsDir = new File(EasyIngestDispatcher.props.getString("deposits-dir")),
    refreshDelay = Duration(EasyIngestDispatcher.props.getInt("refresh-delay"), TimeUnit.MILLISECONDS))

  def init(ctx: DaemonContext): Unit = {
    log.info("Initializing service ...")
  }

  def start(): Unit = {
    log.info("Starting service ...")
    EasyIngestDispatcher.run()
  }

  def stop(): Unit = {
    log.info("Stopping service ...")
    EasyIngestDispatcher.stopTriggered = true
  }

  def destroy(): Unit = {
    EasyIngestDispatcher.waitForQueue()
    log.info("Service stopped.")
  }

}
