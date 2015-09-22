package nl.knaw.dans.easy.ingest_dispatcher

import java.io.{File, PrintWriter, StringWriter}
import java.net.URL
import java.util.concurrent.TimeUnit

import com.yourmediashelf.fedora.client.FedoraCredentials
import nl.knaw.dans.easy.ingest_flow.{DepositState, EasyIngestFlow}
import org.apache.commons.configuration.PropertiesConfiguration
import org.slf4j.LoggerFactory
import rx.lang.scala.Observable

import scala.concurrent.duration._
import scala.io.StdIn.readLine
import scala.language.postfixOps
import scala.util.{Failure, Success, Try}

case class Settings(depositsDir: File, refreshDelay: Duration)

object EasyIngestDispatcher {
  val log = LoggerFactory.getLogger(getClass)

  val homeDir = new File(System.getenv("EASY_INGEST_DISPATCHER_HOME"))
  val props = new PropertiesConfiguration(new File(homeDir, "cfg/application.properties"))

  private var stopTriggered = false
  private var safeToTerminate = false

  def main(args: Array[String]) {
    implicit val s = Settings(
      depositsDir = new File(props.getString("deposits-dir")),
      refreshDelay = Duration(props.getInt("refresh-delay"), TimeUnit.MILLISECONDS))

    jobMonitoringStream
      .doOnError(e => log.error("Error while running ingest-flow", e))
      .retry
      .doOnCompleted {
        log.info("Done, it's safe to terminate now. Please wait for termination...")
        safeToTerminate = true }
      .subscribe(depositId => log.info(s"Finished processing deposit $depositId"))

    log.info(s"Started monitoring deposits in: ${s.depositsDir.getPath}")
    readLine()
    log.info("Stopping monitoring stream, waiting for all jobs to finish")

    stopTriggered = true

    while (!safeToTerminate) {
      Thread.sleep(s.refreshDelay.toMillis)
    }
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
    if(newDeposits.size == 0 && log.isDebugEnabled) log.debug("No new deposits")
    else log.info(s"Processing ${newDeposits.size} new deposits...")
    newDeposits
      .map(dispatchIngestFlow)
      .sequence
  }

  def dispatchIngestFlow(deposit: File): Try[File] = {
    log.info(s"Dispatching ingest-flow for: ${deposit.getName}")
    implicit val s = getIngestFlowSettings(deposit)
    EasyIngestFlow.run()(s).recoverWith {
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

  def setDepositStateToFailed(depositId: String, error: String)(implicit s: EasyIngestFlow.Settings): Try[Unit] =
    DepositState.setDepositState("FAILED", error)


  def isDepositReadyForIngest(deposit: File): Boolean = try {
    deposit.exists() && deposit.isDirectory && depositStateIsSubmitted(deposit)
  } catch {
    case _: Throwable => false
  }

  def depositStateIsSubmitted(deposit: File): Boolean = {
    val stateFile = new File(deposit, "state.properties")
    // If there is no state file, just try to process the deposit (TODO: is this a good solution?)
    !stateFile.exists || new PropertiesConfiguration(stateFile).getString("state") == "SUBMITTED"
  }

  def getIngestFlowSettings(deposit: File): EasyIngestFlow.Settings = {
    EasyIngestFlow.Settings(
      storageUser = props.getString("storage.user"),
      storagePassword = props.getString("storage.password"),
      storageServiceUrl = new URL(props.getString("storage.service-url")),
      fedoraCredentials = new FedoraCredentials(
        props.getString("fcrepo.url"),
        props.getString("fcrepo.user"),
        props.getString("fcrepo.password")),
      numSyncTries = props.getInt("sync.num-tries"),
      syncDelay = props.getInt("sync.delay"),
      ownerId = props.getString("easy.owner"),
      datasetAccessBaseUrl = props.getString("easy.dataset-access-base-url"),
      bagStorageLocation = props.getString("storage.base-url"),
      depositDir = deposit,
      sdoSetDir = new File(props.getString("staging.root-dir"), deposit.getName),
      DOI = "10.1000/xyz123", // TODO: get this from the deposit metadata
      postgresURL = props.getString("fsrdb.connection-url"),
      solr = props.getString("solr.update-url"),
      pidgen = props.getString("pid-generator.url"))
  }

  def getBagDir(depositDir: File): Try[File] = Try {
    depositDir.listFiles.find(f => f.isDirectory && f.getName != ".git").get
  }
}
