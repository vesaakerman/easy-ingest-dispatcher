package nl.knaw.dans.easy

import org.apache.commons.lang.exception.ExceptionUtils._
import scala.util.{Success, Failure, Try}

package object ingest_dispatcher {

  private class CompositeException(throwables: List[Throwable]) extends RuntimeException(throwables.foldLeft("")((msg, t) => s"$msg\n${getMessage(t)} ${getStackTrace(t)}"))

  implicit class ListTryExtensions[T](xs: List[Try[T]]) {
    def sequence: Try[List[T]] =
      if (xs.exists(_.isFailure))
        Failure(new CompositeException(xs.collect { case Failure(e) => e }))
      else
        Success(xs.map(_.get))
  }
}
