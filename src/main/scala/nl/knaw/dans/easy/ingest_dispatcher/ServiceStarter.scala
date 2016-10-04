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

import com.hazelcast.Scala.client._
import com.hazelcast.Scala.serialization
import com.hazelcast.client.config.ClientConfig
import org.apache.commons.configuration.PropertiesConfiguration
import org.apache.commons.daemon.{Daemon, DaemonContext}
import org.slf4j.LoggerFactory

class ServiceStarter extends Daemon {
  val log = LoggerFactory.getLogger(getClass)

  implicit val properties = {
    val ps = new PropertiesConfiguration()
    ps.setDelimiterParsingDisabled(true)
    ps.load(new File(System.getProperty("app.home"), "cfg/application.properties"))

    ps
  }
  implicit val hazelcast = {
    val hzConf = new ClientConfig()
    serialization.Defaults.register(hzConf.getSerializationConfig)
    hzConf.newClient()
  }
  val dispatcher = new EasyIngestDispatcher

  def init(ctx: DaemonContext): Unit = {
    log.info("Initializing service ...")
  }

  def start(): Unit = {
    log.info("Starting service ...")
    dispatcher.run.subscribe()
  }

  def stop(): Unit = {
    log.info("Stopping service ...")
    dispatcher.stop()
  }

  def destroy(): Unit = {
    dispatcher.awaitTermination()
    hazelcast.shutdown()
    log.info("Service stopped.")
  }
}
