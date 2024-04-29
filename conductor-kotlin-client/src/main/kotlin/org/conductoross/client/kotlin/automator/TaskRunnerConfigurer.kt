package org.conductoross.client.kotlin.automator

import org.conductoross.client.kotlin.http.TaskClient
import org.conductoross.client.kotlin.worker.Worker
import com.netflix.discovery.EurekaClient
import io.github.oshai.kotlinlogging.KotlinLogging
import kotlin.time.Duration
import kotlinx.coroutines.*


private val logger = KotlinLogging.logger {}

/** Configures automated polling of tasks and execution via the registered [Worker]s.  */
@OptIn(DelicateCoroutinesApi::class, ExperimentalCoroutinesApi::class)
class TaskRunnerConfigurer internal constructor(builder: TaskRunnerConfigurerBuilder) {
    private val eurekaClient: EurekaClient?
    private val taskClient: TaskClient
    private val workers: MutableList<Worker> = mutableListOf()

    /**
     * @return sleep time in millisecond before task update retry is done when receiving error from
     * the Conductor server
     */
    val sleepWhenRetry: Duration

    /**
     * @return Number of times updateTask should be retried when receiving error from Conductor
     * server
     */
    val updateRetryCount: Int

    /**
     * @return prefix used for worker names
     */
    val workerNamePrefix: String
    private val taskToDomain: Map<String, String>

    private val taskPollExecutor: TaskPollExecutor

    /**
     * @return Thread Count for individual task type
     */
    val taskThreadCount: MutableMap<String, Int>

    /**
     * @return Thread count for leasing tasks
     */
    private val leaseLimitedParallelism: Int

    val workersDispatcher: CoroutineDispatcher

    init {
        for (worker in builder.workers) {
            if (!builder.taskThreadCount.containsKey(worker.taskDefName)) {
                logger.warn {
                    "No thread count specified for task type ${worker.taskDefName}, default to 1 thread"
                }
                builder.taskThreadCount[worker.taskDefName] = 1
            }
            workers.add(worker)
        }

        if (builder.taskThreadCount.isNotEmpty()) {
            val defaultThreadsMap =
                workers.map { it.taskDefName }.subtract(builder.taskThreadCount.keys).associateWith { 1 }
            taskThreadCount = (builder.taskThreadCount + defaultThreadsMap).toMutableMap()
        } else {
            taskThreadCount = mutableMapOf()
        }

        eurekaClient = builder.eurekaClient
        taskClient = builder.taskClient!!
        sleepWhenRetry = builder.sleepWhenRetry
        updateRetryCount = builder.updateRetryCount
        workerNamePrefix = builder.workerNamePrefix
        taskToDomain = builder.taskToDomain
        leaseLimitedParallelism = builder.leaseLimitedParallelism

        workersDispatcher = Dispatchers.Default.limitedParallelism(workers.size)
        val leasingDispatcher = Dispatchers.Default.limitedParallelism(leaseLimitedParallelism)

        taskPollExecutor = TaskPollExecutor(
            eurekaClient,
            taskClient,
            updateRetryCount,
            sleepWhenRetry,
            taskToDomain,
            leasingDispatcher
        )

    }

    /**
     * Starts the polling with channel
     */
    @Synchronized
    fun start() {
        workers.forEach { startWorkerWithChannel(it, workersDispatcher, taskPollExecutor) }
    }

    /**
     * Starts the polling with flow
     */
    @Synchronized
    fun startV2() {
        workers.forEach { startWorkerWithFlow(it, workersDispatcher, taskPollExecutor) }
    }
}