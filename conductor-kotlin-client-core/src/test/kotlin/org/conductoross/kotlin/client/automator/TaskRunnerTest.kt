package org.conductoross.kotlin.client.automator

import com.netflix.conductor.common.metadata.tasks.Task
import com.netflix.conductor.common.metadata.tasks.TaskResult
import io.mockk.coEvery
import io.mockk.coJustRun
import io.mockk.mockk
import kotlin.test.BeforeTest
import kotlin.test.Test
import kotlinx.coroutines.cancel
import org.conductoross.client.kotlin.http.TaskClient
import org.conductoross.client.kotlin.task
import org.conductoross.client.kotlin.worker.Worker

class TaskRunnerTest {
    private lateinit var client: TaskClient
    @BeforeTest
    fun setup() {
        client = mockk()
    }

    @Test
    fun testStartWorkerWithChannel() {
        coEvery {
            client.batchPollTasksInDomain("task_1", null, any(), 2, 1000)
        } returns listOf(task("task_1"), task("task_1"))

        coEvery {
            client.batchPollTasksInDomain("task_2", null, any(), 2, 1000)
        } returns listOf(task("task_2"), task("task_2"))

        coEvery {
            client.evaluateAndUploadLargePayload(any(), any())
        } returns null

//        coEvery { client.updateTask(any()) } throws RuntimeException("Io exception")

        coJustRun {
            client.updateTask(any())
        }

        val worker1 = Worker.create("task_1") { task: Task? -> TaskResult(task) }
        val worker2 = Worker.create("task_2") { task: Task? -> TaskResult(task) }
        val configurer = TaskRunnerConfigurer {
            taskClient = client
            addWorkers(listOf(worker1, worker2))
        }
        configurer.startChannel()
        Thread.sleep(3000)
        configurer.workerScopes.filterKeys { it.taskDefName == "task_1" }.forEach { (_, scope) -> scope.cancel() }
//        configurer.taskRunnerScope.cancel()
        Thread.sleep(10000)
    }
}