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

import com.hazelcast.Scala.client._
import com.hazelcast.Scala.serialization
import com.hazelcast.client.config.ClientConfig
import org.apache.commons.configuration.PropertiesConfiguration
import org.slf4j.LoggerFactory

/**
 * Temporary interface to the service, for use by ServiceStarter, so that we can have a REST and Hazelcast mode
 * in one service.
 */
trait Service {
  def start(): Unit

  def stop(): Unit

  def destroy(): Unit
}

class RestService(implicit properties: PropertiesConfiguration) extends Service {

  val log = LoggerFactory.getLogger(getClass)
  log.info("Initializing REST ingest-dispatcher service...")

  val dispatcher = new RestIngestDispatcher

  override def start() = {
    log.info("Starting REST ingest-dispatcher service...")
    dispatcher.run.subscribe()
  }

  override def stop() = {
    log.info("Stopping REST ingest-dispatcher service...")
    dispatcher.stop()
  }

  override def destroy() = {
    dispatcher.awaitTermination()
    log.info("Service REST ingest-dispatcher stopped.")
  }
}

class HazelcastService(implicit properties: PropertiesConfiguration) extends Service {

  val log = LoggerFactory.getLogger(getClass)
  log.info("Initializing Hazelcast ingest-dispatcher service...")

  implicit val hazelcast = {
    val hzConf = new ClientConfig()
    serialization.Defaults.register(hzConf.getSerializationConfig)
    hzConf.newClient()
  }
  val dispatcher = new HazelcastIngestDispatcher

  override def start() = {
    log.info("Starting Hazelcast ingest-dispatcher service...")
    dispatcher.run.subscribe()
  }

  override def stop() = {
    log.info("Stopping Hazelcast ingest-dispatcher service...")
    dispatcher.stop()
  }

  override def destroy() = {
    dispatcher.awaitTermination()
    hazelcast.shutdown()
    log.info("Service Hazelcast ingest-dispatcher stopped.")
  }
}
