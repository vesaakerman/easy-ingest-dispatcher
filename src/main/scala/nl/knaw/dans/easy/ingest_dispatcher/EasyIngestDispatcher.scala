package nl.knaw.dans.easy.ingest_dispatcher

import java.io.File
import java.net.URL

import com.yourmediashelf.fedora.client.FedoraCredentials
import nl.knaw.dans.easy.ingest_flow.EasyIngestFlow
import org.apache.commons.configuration.PropertiesConfiguration
import org.eclipse.jgit.api.Git
import org.slf4j.LoggerFactory
import rx.lang.scala.Observable

import scala.annotation.tailrec
import scala.concurrent.duration._
import scala.io.StdIn.readLine
import scala.language.postfixOps
import scala.util.{Failure, Success, Try}

case class Settings(bagsFolder: File, refreshDelay: Duration, fedoraCredentials: FedoraCredentials)

object EasyIngestDispatcher {
  val log = LoggerFactory.getLogger(getClass)

  private var stopTriggered = false
  private var safeToTerminate = false

  def main(args: Array[String]) {
    implicit val s = Settings(
      bagsFolder = new File("/Users/georgik/git/service/easy/easy-deposit/data"),
      refreshDelay = 5 seconds,
      fedoraCredentials = new FedoraCredentials("http://deasy:8080/fedora", "fedoraAdmin", "fedoraAdmin"))

    jobMonitoringStream
      .doOnError(e => log.error("Error while running ingest-flow", e))
      .retry
      .doOnCompleted {
        log.info("Done, it's safe to terminate now. Please wait for termination...")
        safeToTerminate = true }
      .subscribe(depositId => log.info(s"Finished processing deposit $depositId"))

    log.info(s"Started monitoring deposits in: ${s.bagsFolder.getPath}")
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
    val bags = s.bagsFolder.listFiles().toList
    bags.diff(processedBags)
      .filter(isBagReadyForIngest)
      .map(dispatchIngestFlow)
      .sequence
  }

  def dispatchIngestFlow(bag: File): Try[File] = {
    log.info(s"Dispatching ingest-flow for: ${bag.getName}")
    for {
      bagRoot <- findBagitRoot(bag)
      settings = getIngestFlowSettings(bagRoot)
      _ <- EasyIngestFlow.run()(settings)
    } yield bag
  }

  def isBagReadyForIngest(bag: File): Boolean = try {
    bag.exists() && bag.isDirectory && Git.open(bag).tagList().call().size() == 1
  } catch {
    case _: Throwable => false
  }

  def getIngestFlowSettings(bag: File): EasyIngestFlow.Settings = {
    val homeDir = new File(System.getenv("EASY_INGEST_DISPATCHER_HOME"))
    val props = new PropertiesConfiguration(new File(homeDir, "cfg/application.properties"))
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
      bagStorageLocation = props.getString("storage.base-url"),
      bagitDir = bag,
      sdoSetDir = new File(props.getString("staging.root-dir"), bag.getName),
      DOI = "10.1000/xyz123", // TODO: get this from the deposit metadata
      postgresURL = props.getString("fsrdb.connection-url"),
      solr = props.getString("solr.update-url"),
      pidgen = props.getString("pid-generator.url"))
  }

  @tailrec
  private def findBagitRoot(f: File): Try[File] =
    if (f.isDirectory) {
      val children = f.listFiles.filter(_.getName != ".git")
      if (children.length == 1) {
        findBagitRoot(children.head)
      } else if (children.length > 1) {
        Success(f)
      } else {
        Failure(new RuntimeException(s"Bagit folder seems to be empty in: ${f.getName}"))
      }
    } else {
      Failure(new RuntimeException(s"Couldn't find bagit folder, instead found: ${f.getName}"))
    }

}
