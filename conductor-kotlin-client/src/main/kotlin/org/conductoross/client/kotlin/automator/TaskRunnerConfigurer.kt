package org.conductoross.client.kotlin.automator

import org.conductoross.client.kotlin.http.TaskClient
import org.conductoross.client.kotlin.worker.Worker
import com.netflix.discovery.EurekaClient
import io.github.oshai.kotlinlogging.KotlinLogging
import kotlin.math.min
import kotlin.time.Duration
import kotlinx.coroutines.*

const val MIN_WORKERS_PARALLELISM = 8
private val logger = KotlinLogging.logger {}

/** Configures automated polling of tasks and execution via the registered [Worker]s.  */
@OptIn(ExperimentalCoroutinesApi::class)
class TaskRunnerConfigurer internal constructor(builder: TaskRunnerConfigurerBuilder) {
    private val eurekaClient: EurekaClient?
    private val taskClient: TaskClient
    private val workers: Collection<Worker>
    val workerScopes: Map<Worker, CoroutineScope>

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
     * @return Thread count for leasing tasks
     */
    private val leaseLimitedParallelism: Int

    val workersDispatcher: CoroutineDispatcher

    val taskRunnerScope: CoroutineScope

    init {
        workers = builder.workers

        eurekaClient = builder.eurekaClient
        taskClient = builder.taskClient!!
        sleepWhenRetry = builder.sleepWhenRetry
        updateRetryCount = builder.updateRetryCount
        workerNamePrefix = builder.workerNamePrefix
        taskToDomain = builder.taskToDomain
        leaseLimitedParallelism = builder.leaseLimitedParallelism

        workersDispatcher = Dispatchers.Default.limitedParallelism(min(MIN_WORKERS_PARALLELISM, workers.size))
        taskRunnerScope = CoroutineScope(SupervisorJob() + workersDispatcher)
//       workerScopes = workers.associateBy(
//            { it.taskDefName },
//            { workerScope(workersDispatcher, taskRunnerScope, it) }
//        )
        workerScopes = workers.associateWith { workerScope(workersDispatcher, taskRunnerScope, it) }
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
    fun startChannel() {
        workerScopes.forEach { startWorkerWithChannel(it.key, workersDispatcher, taskPollExecutor, it.value) }
//        workers.forEach { startWorkerWithChannel(it, workersDispatcher, taskPollExecutor, taskRunnerScope) }
    }

    /**
     * Starts the polling with flow
     */
    @Synchronized
    fun startFlow() {
        workers.forEach { startWorkerWithFlow(it, workersDispatcher, taskPollExecutor, taskRunnerScope) }
    }

    fun shutdown() {
        taskRunnerScope.cancel()
    }
}