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

import nl.knaw.dans.easy.ingest_flow.{Hazelcast, PidGeneratorMode, Rest}
import org.apache.commons.configuration.PropertiesConfiguration
import org.apache.commons.daemon.{Daemon, DaemonContext}
import org.slf4j.{Logger, LoggerFactory}

class ServiceStarter extends Daemon {
  var log: Logger = _
  var service: Service = _

  private def getProperties = {
    val ps = new PropertiesConfiguration()
    ps.setDelimiterParsingDisabled(true)
    ps.load(new File(System.getProperty("app.home"), "cfg/application.properties"))

    ps
  }

  def init(ctx: DaemonContext) = {
    log = LoggerFactory.getLogger(getClass)

    log.info("Initializing service...")

    implicit val properties = getProperties
    service = PidGeneratorMode.getMode(properties.getString("microservice.pid-generator.mode")) match {
      case Rest => new RestService
      case Hazelcast => new HazelcastService
    }

    log.info("Service initialized.")
  }

  def start(): Unit = {
    log.info("Starting service...")
    service.start()
    log.info("Service started.")
  }

  def stop(): Unit = {
    log.info("Stopping service...")
    service.stop()
  }

  def destroy(): Unit = {
    service.destroy()
    log.info("Service stopped.")
  }
}
