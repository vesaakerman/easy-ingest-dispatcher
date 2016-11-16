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

import nl.knaw.dans.easy.ingest_flow.State._

/**
 * Created by vesa on 11/15/16.
 */
object State  extends Enumeration {
  type State = Value
  val ARCHIVED, DRAFT, FAILED, FINALIZING, INVALID, REJECTED, SUBMITTED = Value

  def stringValues = this.values.map(_.toString)
}