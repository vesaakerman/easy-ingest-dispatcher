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

import java.io.{File, FileNotFoundException, PrintWriter, StringWriter}
import java.net.URL
import java.util.concurrent.CountDownLatch
import java.util.concurrent.atomic.AtomicBoolean
import javax.naming.Context
import javax.naming.ldap.InitialLdapContext

import com.hazelcast.core.HazelcastInstance
import com.yourmediashelf.fedora.client.FedoraCredentials
import nl.knaw.dans.easy.ingest_flow.{EasyIngestFlow, Hazelcast, MicroserviceSettings, Rest, setDepositState, Settings => IngestFlowSettings}
import org.apache.commons.configuration.PropertiesConfiguration
import org.slf4j.LoggerFactory
import rx.lang.scala.Observable

import scala.concurrent.duration._
import scala.language.postfixOps
import scala.util.{Failure, Try}

class EasyIngestDispatcher(implicit props: PropertiesConfiguration, hazelcast: HazelcastInstance) {
  val log = LoggerFactory.getLogger(getClass)
  val depositsDir = {
    val dirLocation = props.getString("deposits-dir")
    Option(new File(dirLocation))
      .filter(_.exists)
      .getOrElse {
        log.error(s"Directory at location $dirLocation does not exist + exception is thrown")
        throw new FileNotFoundException(s"Directory at location $dirLocation does not exist")
      }
  }
  val refreshDelay = props.getInt("refresh-delay") milliseconds

  private val running = new AtomicBoolean(true)
  private val safeToTerminate = new CountDownLatch(1)

  def stop() = running.compareAndSet(true, false)

  def awaitTermination(): Unit = {
    log.info("Processing remaining queue items before terminating ...")
    safeToTerminate.await()
    log.info("Queue empty. Shutting down ...")
  }

  def run: Observable[DepositName] = {
    jobMonitoringStream
      .doOnError(e => log.error("Error while running ingest-flow", e))
      .retry
      .doOnNext(depositId => log.info(s"Finished processing deposit $depositId"))
      .doOnCompleted {
        log.info("Done, it's safe to terminate now. Please wait for termination...")
        safeToTerminate.countDown()
      }
      .doOnSubscribe {
        log.info(s"Started monitoring deposits in: ${depositsDir.getPath}")
      }
  }

  def jobMonitoringStream: Observable[DepositName] = {
    Observable.interval(refreshDelay)
      .onBackpressureDrop
      .takeWhile(_ => running.get())
      .sample(refreshDelay)
      .scan(initProcessedDeposits)((processedDeposits, _) => processedDeposits ++ dispatchUnprocessedDeposits(processedDeposits))
      .flatMapIterable(_.map(_.getName))
      .distinct
  }

  def initProcessedDeposits: List[Deposit] = {
    depositsDir.listFiles().filterNot(isDepositReadyForIngest).toList
  }

  def dispatchUnprocessedDeposits(processedDeposits: List[Deposit]): List[Deposit] = {
    val deposits = depositsDir.listFiles().toList
    val newDeposits = deposits.diff(processedDeposits)
      .filter(isDepositReadyForIngest)
    if(newDeposits.nonEmpty) log.info(s"Processing ${newDeposits.size} new deposits ...")
    else log.debug("No new deposits ...")

    newDeposits.foreach(dispatchIngestFlow)
    newDeposits
  }

  def dispatchIngestFlow(deposit: Deposit): Unit = {
    log.info(s"Dispatching ingest-flow for: ${deposit.getName}")
    implicit val s = getIngestFlowSettings(deposit)

    // TODO candidate for Try.onError in dans-scala-lib rather than this Try.recoverWith
    // TODO dirty solution with toBlocking, but can't think of anything that works fine in the context of the jobMonitoringStream :(
    Try(EasyIngestFlow.run.toBlocking.toList.head).recoverWith {
      case t =>
        log.error("Ingest flow failed", t)
        val sw = new StringWriter()
        val pw = new PrintWriter(sw)
        t.printStackTrace(pw)
        pw.flush()
        setDepositStateToFailed(sw.toString)
        Failure(t)
    }
  }

  def setDepositStateToFailed(error: String)(implicit s: IngestFlowSettings): Try[Unit] =
    setDepositState("FAILED", error)

  def isDepositReadyForIngest(deposit: Deposit): Boolean = {
    Try {
      deposit.exists && deposit.isDirectory && depositStateIsSubmitted(deposit)
    }.getOrElse(false)
  }

  def depositStateIsSubmitted(deposit: Deposit): Boolean = {
    val stateFile = new File(deposit, "deposit.properties")
    stateFile.isFile && new PropertiesConfiguration(stateFile).getString("state.label") == "SUBMITTED"
  }

  def getIngestFlowSettings(deposit: Deposit): IngestFlowSettings = {
    def getMode(mode: String) = {
      mode match {
        case "rest" => Rest
        case "hazelcast" => Hazelcast
      }
    }

    IngestFlowSettings(
      storageUser = props.getString("storage.user"),
      storagePassword = props.getString("storage.password"),
      storageServiceUrl = new URL(props.getString("storage.service-url")),
      fedoraCredentials = new FedoraCredentials(
        props.getString("fcrepo.url"),
        props.getString("fcrepo.user"),
        props.getString("fcrepo.password")),
      ldapContext = {
        import java.{util => ju}

        val env = new ju.Hashtable[String, String]
        env.put(Context.PROVIDER_URL, props.getString("auth.ldap.url"))
        env.put(Context.SECURITY_AUTHENTICATION, "simple")
        env.put(Context.SECURITY_PRINCIPAL, props.getString("auth.ldap.user"))
        env.put(Context.SECURITY_CREDENTIALS, props.getString("auth.ldap.password"))
        env.put(Context.INITIAL_CONTEXT_FACTORY, "com.sun.jndi.ldap.LdapCtxFactory")

        new InitialLdapContext(env, null)
      },
      numSyncTries = props.getInt("sync.num-tries"),
      syncDelay = props.getInt("sync.delay"),
      checkInterval = props.getInt("check.interval"),
      maxCheckCount = props.getInt("max.check.count"),
      ownerId = getUserId(deposit),
      customDatasetLicenseText = props.getString("custom-dataset-license-text"),
      datasetAccessBaseUrl = props.getString("easy.dataset-access-base-url"),
      depositDir = deposit,
      licenseResourceDir = new File(props.getString("license.resources")),
      sdoSetDir = new File(props.getString("staging.root-dir"), deposit.getName),
      postgresURL = props.getString("fsrdb.connection-url"),
      solr = props.getString("solr.update-url"),
      pidgen = props.getString("pid-generator.url"),
      virusscanCmd = props.getString("virusscan.cmd"),
      microserviceSettings = MicroserviceSettings(
        ingestflowResponseMapName = props.getString("microservice.ingest-flow.response-map"),
        pidGeneratorInboxName = props.getString("microservice.pid-generator.inbox"),
        pidGeneratorMode = getMode(props.getString("microservice.pid-generator.mode")
      )))
  }

  def getUserId(deposit: Deposit): String = {
    val ps = new PropertiesConfiguration()
    ps.setDelimiterParsingDisabled(true)
    ps.load(new File(deposit, "deposit.properties"))
    ps.getString("depositor.userId")
  }
}
