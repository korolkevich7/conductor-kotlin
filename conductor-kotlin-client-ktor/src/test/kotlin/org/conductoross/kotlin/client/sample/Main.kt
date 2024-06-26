/*
 * <p>
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 */
package org.conductoross.kotlin.client.sample

import org.conductoross.kotlin.client.automator.TaskRunnerConfigurer
import org.conductoross.kotlin.client.http.TaskClient
import org.conductoross.kotlin.client.http.ktor.KtorTaskClient
import io.ktor.client.*
import io.ktor.client.engine.cio.*

fun main() {
    val ktorTaskClient: TaskClient = KtorTaskClient("http://localhost:8080/api/", HttpClient(CIO))
//    val threadCount = 2 // number of threads used to execute workers.  To avoid starvation, should be
    // same or more than number of workers
    val worker1 = SampleWorker("task_1")
    val worker2 = SampleWorker("task_5")

    // Create TaskRunnerConfigurer
    val configurer = TaskRunnerConfigurer {
        taskClient = ktorTaskClient
        addWorkers(listOf(worker1, worker2))
    }

    // Start the polling and execution of tasks
    configurer.startChannel()
}
