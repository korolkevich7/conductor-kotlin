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
package org.conductoross.kotlin.client.automator

import com.netflix.conductor.common.metadata.tasks.Task
import com.netflix.conductor.common.metadata.tasks.TaskResult
import io.mockk.mockk
import kotlin.test.BeforeTest
import kotlin.test.Test
import org.conductoross.kotlin.client.http.TaskClient
import org.conductoross.kotlin.client.worker.Worker.Companion.create
import java.util.*

class TaskRunnerConfigurerTest {
    private lateinit var client: TaskClient
    @BeforeTest
    fun setup() {
        client = mockk()
    }

    @Test//(expected = ConductorClientException::class)
    fun testInvalidThreadConfig() {
        val worker1 = create("task1") { task: Task? -> TaskResult(task) }
        val worker2 = create("task2") { task: Task? -> TaskResult(task) }
        TaskRunnerConfigurer {
            taskClient = client
            addWorkers(listOf(worker1, worker2))
        }
    }
}
