package nl.knaw.dans.easy

import scala.util.{Success, Failure, Try}

package object ingest_dispatcher {
  class CompositeException(throwables: List[Throwable])
    extends RuntimeException(throwables.foldLeft("")((msg, t) => s"${msg}*** ERROR:\n${t.getMessage}\n${t.getStackTrace.mkString("\n")}"))

  implicit class ListTryExtensions[T](xs: List[Try[T]]) {
    def sequence: Try[List[T]] =
      if (xs.exists(_.isFailure))
        Failure(new CompositeException(xs.collect { case Failure(e) => e }))
      else
        Success(xs.map(_.get))
  }
}
