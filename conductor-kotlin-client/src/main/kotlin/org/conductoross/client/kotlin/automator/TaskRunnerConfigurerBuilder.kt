package org.conductoross.client.kotlin.automator

import com.netflix.discovery.EurekaClient
import kotlin.time.Duration.Companion.milliseconds
import org.conductoross.client.kotlin.http.TaskClient
import org.conductoross.client.kotlin.worker.Worker

/** Builder used to create the instances of TaskRunnerConfigurer  */
class TaskRunnerConfigurerBuilder {
    //TODO
    /**
     * prefix to be used for worker names, defaults to workflow-worker-
     * if not supplied.
     */
    var workerNamePrefix = "workflow-worker"

    /**
     * time in milliseconds, for which the thread should sleep when task
     * update call fails, before retrying the operation.
     */
    var sleepWhenRetry = 500.milliseconds

    /**
     * number of times to retry the failed updateTask operation
     * @see sleepWhenRetry
     */
    var updateRetryCount = 3

    /**
     * limited threads for lease extend tasks
     */
    var leaseLimitedParallelism = 2
    internal var workers: Collection<Worker> = emptyList()

    /**
     * Eureka client - used to identify if the server is in discovery or
     * not. When the server goes out of discovery, the polling is terminated. If passed
     * null, discovery check is not done.
     */
    var eurekaClient: EurekaClient? = null
    var taskClient: TaskClient? = null
    var taskToDomain: Map<String, String> = mapOf()

    fun addWorker(worker: Worker) {
        workers += worker
    }

    fun addWorkers(workers: Collection<Worker>) {
        this.workers += workers
    }

    /**
     * Builds an instance of the TaskRunnerConfigurer.
     *
     *
     * Please see [TaskRunnerConfigurer.startChannel] method. The method must be called after
     * this constructor for the polling to start.
     */
    internal fun build(): TaskRunnerConfigurer {
        require(workers.isNotEmpty()) { "Workers cannot be empty" }
        requireNotNull(taskClient) { "task client should not be null" }
        return TaskRunnerConfigurer(this)
    }
}

fun TaskRunnerConfigurer(block: TaskRunnerConfigurerBuilder.() -> Unit): TaskRunnerConfigurer {
    val builder = TaskRunnerConfigurerBuilder()
    builder.block()
    return builder.build()
}