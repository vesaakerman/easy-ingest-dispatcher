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

import java.io.{File, PrintWriter, StringWriter}
import java.net.URL
import javax.naming.Context
import javax.naming.ldap.InitialLdapContext

import com.yourmediashelf.fedora.client.FedoraCredentials
import nl.knaw.dans.easy.ingest_flow.{EasyIngestFlow, setDepositState, Settings => IngestFlowSettings}
import nl.knaw.dans.lib.error._
import org.apache.commons.configuration.PropertiesConfiguration
import org.slf4j.LoggerFactory
import rx.lang.scala.Observable

import scala.concurrent.duration._
import scala.language.postfixOps
import scala.util.{Failure, Success, Try}

case class Settings(depositsDir: File, refreshDelay: Duration)

object EasyIngestDispatcher {
  val log = LoggerFactory.getLogger(getClass)

  val homeDir = new File(System.getProperty("app.home"))
  val props = {
    val ps = new PropertiesConfiguration()
    ps.setDelimiterParsingDisabled(true)
    ps.load(new File(homeDir, "cfg/application.properties"))

    ps
  }

  var stopTriggered = false
  private var safeToTerminate = false

  def run()(implicit s: Settings) {
    jobMonitoringStream
      .doOnError(e => log.error("Error while running ingest-flow", e))
      .retry
      .doOnCompleted {
        log.info("Done, it's safe to terminate now. Please wait for termination...")
        safeToTerminate = true }
      .subscribe(depositId => log.info(s"Finished processing deposit $depositId"))

    log.info(s"Started monitoring deposits in: ${s.depositsDir.getPath}")
  }

  def waitForQueue()(implicit s: Settings): Unit = {
    log.info("Processing remaining queue items before terminating ...")
    while (!safeToTerminate)
      Thread.sleep(s.refreshDelay.toMillis)
    log.info("Queue empty. Shutting down ...")
  }

  def jobMonitoringStream(implicit s: Settings): Observable[String] = {
    Observable.interval(s.refreshDelay)
      .onBackpressureDrop
      .takeWhile(_ => !stopTriggered)
      .sample(s.refreshDelay)
      .scan(List[File]()) ((processedBags, _) => processedBags ++ dispatchUnprocessedBags(processedBags).get)
      .flatMapIterable(_.map(_.getName))
      .distinct
  }

  def dispatchUnprocessedBags(processedBags: List[File])(implicit s: Settings): Try[List[File]] = {
    val bags = s.depositsDir.listFiles().toList
    val newDeposits = bags.diff(processedBags)
      .filter(isDepositReadyForIngest)
    if(newDeposits.nonEmpty) log.info(s"Processing ${newDeposits.size} new deposits ...")
    else log.debug("No new deposits ...")
    newDeposits
      .map(dispatchIngestFlow)
      .collectResults
  }

  def dispatchIngestFlow(deposit: File): Try[File] = {
    log.info(s"Dispatching ingest-flow for: ${deposit.getName}")
    implicit val s = getIngestFlowSettings(deposit)
    EasyIngestFlow.run.recoverWith {
      case t =>
        log.error("Ingest flow failed", t)
        val sw = new StringWriter()
        val pw = new PrintWriter(sw)
        t.printStackTrace(pw)
        pw.flush()
        setDepositStateToFailed(deposit.getName, sw.toString)
        Failure(t)
    }
    Success(deposit)
  }

  def setDepositStateToFailed(depositId: String, error: String)(implicit s: IngestFlowSettings): Try[Unit] =
    setDepositState("FAILED", error)


  def isDepositReadyForIngest(deposit: File): Boolean = try {
    deposit.exists && deposit.isDirectory && depositStateIsSubmitted(deposit)
  } catch {
    case _: Throwable => false
  }

  def depositStateIsSubmitted(deposit: File): Boolean = {
    val stateFile = new File(deposit, "deposit.properties")
    stateFile.isFile && new PropertiesConfiguration(stateFile).getString("state.label") == "SUBMITTED"
  }

  def getIngestFlowSettings(deposit: File): IngestFlowSettings = {
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
      virusscanCmd = props.getString("virusscan.cmd"))
  }

  def getUserId(depositDir: File): String = new PropertiesConfiguration(new File(depositDir, "deposit.properties")).getString("depositor.userId")
}
