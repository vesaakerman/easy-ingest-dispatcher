import java.io.File

import scala.util.Try

case class Settings(depositsDir: File)

object EasyIngestDispatcher {
  def main(args: Array[String]) {

  }

  def run(implicit s: Settings): Try[Unit] = {
    ???
  }
}
