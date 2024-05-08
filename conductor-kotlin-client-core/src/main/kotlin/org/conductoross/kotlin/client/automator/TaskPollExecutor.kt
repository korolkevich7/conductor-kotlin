package org.conductoross.kotlin.client.automator

import com.netflix.appinfo.InstanceInfo
import org.conductoross.kotlin.client.config.PropertyFactory
import org.conductoross.kotlin.client.exception.ConductorTimeoutClientException
import org.conductoross.kotlin.client.http.TaskClient
import org.conductoross.kotlin.client.worker.Worker
import com.netflix.conductor.common.metadata.tasks.Task
import com.netflix.conductor.common.metadata.tasks.TaskResult
import com.netflix.conductor.common.metadata.tasks.responseTimeoutFromNow
import com.netflix.discovery.EurekaClient
import io.github.oshai.kotlinlogging.KotlinLogging
import kotlinx.coroutines.*
import kotlin.time.Duration
import kotlin.time.Duration.Companion.seconds
import kotlin.time.ExperimentalTime
import kotlin.time.measureTimedValue
import org.conductoross.kotlin.client.telemetry.MetricsContainer
import org.conductoross.kotlin.client.telemetry.incrementTaskExecutionErrorCount
import org.conductoross.kotlin.client.telemetry.incrementTaskLeaseExtendCount
import org.conductoross.kotlin.client.telemetry.incrementTaskLeaseExtendErrorCount
import org.conductoross.kotlin.client.telemetry.incrementTaskPausedCount
import org.conductoross.kotlin.client.telemetry.incrementTaskPollCount
import org.conductoross.kotlin.client.telemetry.incrementTaskUpdateErrorCount
import org.conductoross.kotlin.client.telemetry.recordExecutionTimer
import org.conductoross.kotlin.client.telemetry.recordPollTimer

private val logger = KotlinLogging.logger {}

/**
 * Manages the threadpool used by the workers for execution and server communication (polling and
 * task update).
 */
class TaskPollExecutor(
    private val eurekaClient: EurekaClient?,
    private val taskClient: TaskClient,
    private val updateRetryCount: Int,
    private val sleepRetryInterval: Duration,
    private val taskToDomain: Map<String, String>,
    private val leaseExtendDispatcher: CoroutineDispatcher
) {

    init {
        //TODO Coroutine monitor???
//        ThreadPoolMonitor.attach(REGISTRY, executorService, workerNamePrefix)
    }

    @OptIn(ExperimentalTime::class)
    internal suspend fun poll(worker: Worker): List<Task> {
        requireNotNull(worker.identity) { "Worker identity cannot be null" }

        val discoveryOverride = isDiscoveryOverride(worker.taskDefName)
        if (eurekaClient != null && eurekaClient.instanceRemoteStatus != InstanceInfo.InstanceStatus.UP
            && !discoveryOverride
        ) {
            logger.debug { "Instance is NOT UP in discovery - will not poll" }
            return emptyList()
        }
        if (worker.paused()) {
            MetricsContainer.incrementTaskPausedCount(worker.taskDefName)
            logger.debug { "Worker ${worker::class} has been paused. Not polling anymore!" }
            return emptyList()
        }

        val taskType: String = worker.taskDefName
        val domain = taskDomain(taskType)
        logger.debug { "Polling task of type: $taskType in domain: '$domain'" }
        val (tasks, duration) = measureTimedValue {
            taskClient.batchPollTasksInDomain(
                taskType,
                domain,
                worker.identity!!,
                worker.batchPollCount,
                worker.batchPollTimeout.inWholeMilliseconds
            )
        }
        MetricsContainer.recordPollTimer(taskType, duration)
        return tasks
    }

////todo coroutine uncaughtExceptionHandler
//@OptIn(DelicateCoroutinesApi::class)
//val handler = CoroutineExceptionHandler{ context, exception ->
//    GlobalScope.launch {
//        MetricsContainer.incrementUncaughtExceptionCount()
//    }
//    logger.error(exception) { "Uncaught exception. Context $context will exit now" }
//}


    internal suspend fun processTask(worker: Worker, task: Task): TaskWithResult {
        MetricsContainer.incrementTaskPollCount(task.taskType, 1)
        val domain = taskDomain(task.taskType)
        val taskResponseTimeoutSeconds = task.responseTimeoutSeconds
        logger.debug {
            "Polled task: ${task.taskId} of type: ${task.taskType} in domain: '$domain', from worker: ${worker.identity}" }
        //TODO
        val responseTimeout = task.responseTimeoutFromNow
        try {
            return withTimeout(responseTimeout) {
                val deferred = async {
                    executeTask(worker, task)
                }
                val interval = (taskResponseTimeoutSeconds * LEASE_EXTEND_DURATION_FACTOR).seconds
                var extendLeaseJob: Job? = null
                if (taskResponseTimeoutSeconds > 0 && worker.leaseExtendEnabled) {
                    extendLeaseJob = launchExtendLease(interval, task, deferred)
                }
                val result = deferred.await()
                logger.debug {
                    "Task:${task.taskId} of type:${task.taskDefName} finished processing with status:${result.first.status}" }
                extendLeaseJob?.cancel()
                result
            }
        } catch (e: TimeoutCancellationException) {
            throw ConductorTimeoutClientException("Task:${task.taskId} of type:${task.taskDefName} canceled with timeout: $responseTimeout")
        }

        //TODO: should update after it
    }

    @OptIn(ExperimentalTime::class)
    private suspend fun executeTask(worker: Worker, task: Task): TaskWithResult {
        logger.debug {
            "Executing task: ${task.taskId} of type: ${task.taskDefName} in worker: ${worker::class.simpleName ?: "Undefined"} at ${worker.identity}" }
        val (result, duration) = measureTimedValue {
            _executeTask(worker, task)
        }
        MetricsContainer.recordExecutionTimer(worker.taskDefName, duration)
        logger.debug {
            "Task: ${task.taskId} executed by worker: ${worker::class.simpleName ?: "Undefined"} at ${worker.identity} with status: ${result.second.status}" }
        return result
    }

    private suspend fun _executeTask(worker: Worker, task: Task): TaskWithResult {
        return try {
            val result = worker.execute(task)
            result.workflowInstanceId = task.workflowInstanceId
            result.taskId = task.taskId
            result.workerId = worker.identity
            task to result
        } catch (e: Exception) {
            logger.error(e) {
                "Unable to execute task: ${task.taskId} of type: ${task.taskDefName}"
            }
            val result = handleException(e, worker, task)
            task to result
        }
    }

    private suspend fun updateTaskResult(
            count: Int,
            delay: Duration,
            task: Task,
            result: TaskResult,
            worker: Worker
    ) {
        try {
            // upload if necessary
            val externalStorageLocation = retryIO(times = count, initialDelay = delay.inWholeMilliseconds) {
                upload(result, task.taskType)
            }
            externalStorageLocation?.let {
                result.externalOutputPayloadStoragePath = externalStorageLocation
                result.outputData = null
            }
            retryIO(times = count) {
                taskClient.updateTask(result)
            }
        } catch (e: Exception) {
            worker.onErrorUpdate(task)
            MetricsContainer.incrementTaskUpdateErrorCount(
                    worker.taskDefName,
                    e
            )
            logger.error(e) { "Failed to update result: $result for task: ${task.taskDefName} in worker: ${worker.identity}" }
        }
    }

    private suspend fun upload(result: TaskResult, taskType: String): String? {
        return try {
            taskClient.evaluateAndUploadLargePayload(result.outputData, taskType)
        } catch (iae: IllegalArgumentException) {
            result.reasonForIncompletion = iae.message
            result.outputData = null
            result.status = TaskResult.Status.FAILED_WITH_TERMINAL_ERROR
            null
        }
    }

    private suspend fun <T> retryIO(
        times: Int,
        initialDelay: Long = 500, // 0.5 second
        maxDelay: Long = initialDelay,
        factor: Double = 1.0,
        block: suspend () -> T): T
    {
        var currentDelay = initialDelay
        repeat(times - 1) {
            try {
                return block()
            } catch (ex: Exception) {
                logger.warn(ex) { "Operation failed with $it attempt" }
            }
            delay(currentDelay)
            currentDelay = (currentDelay * factor).toLong().coerceAtMost(maxDelay)
        }
        return block() // last attempt
    }

    private suspend fun handleException(
        t: Throwable,
        worker: Worker,
        task: Task
    ): TaskResult {
        val result = TaskResult(task)
        logger.error(t) { "Error while executing task $task" }
        MetricsContainer.incrementTaskExecutionErrorCount(
                worker.taskDefName,
                t
        )
        task.status = Task.Status.FAILED
        result.status = TaskResult.Status.FAILED
        result.reasonForIncompletion = "Error while executing the task: $t"
        result.logStackTrace(t)
        return result
    }

    internal suspend fun updateTaskResult(task: Task, result: TaskResult, worker: Worker) {
        updateTaskResult(updateRetryCount, sleepRetryInterval, task, result, worker)
    }

    @OptIn(ExperimentalTime::class)
    private suspend fun launchExtendLease(interval: Duration, task: Task, taskDeferred: Job): Job =
        coroutineScope {
            timer(interval, interval, leaseExtendDispatcher) {
                extendLease(task, taskDeferred)
            }
        }

    private suspend fun extendLease(task: Task, taskDeferred: Job) {
        if (taskDeferred.isCompleted) {
            logger.warn {
                "Task processing for ${task.taskId} completed, but its lease extend was not cancelled" }
            return
        }
        logger.info { "Attempting to extend lease for ${task.taskId}" }
        try {
            val result = TaskResult(task)
            result.isExtendLease = true
            retryIO(times = LEASE_EXTEND_RETRY_COUNT, sleepRetryInterval.inWholeMilliseconds) {
                taskClient.updateTask(result)
            }
            MetricsContainer.incrementTaskLeaseExtendCount(
                task.taskDefName,
                1
            )
        } catch (e: Exception) {
            MetricsContainer.incrementTaskLeaseExtendErrorCount(
                task.taskDefName,
                e
            )
            logger.error(e) { "Failed to extend lease for ${task.taskId}" }
        }
    }

    private fun isDiscoveryOverride(taskDefName: String): Boolean {
        return PropertyFactory.getBoolean(taskDefName, OVERRIDE_DISCOVERY)
            ?: PropertyFactory.getBoolean(ALL_WORKERS, OVERRIDE_DISCOVERY) ?: false
    }

    private fun taskDomain(taskType: String): String? {
        return PropertyFactory.getString(taskType, DOMAIN)
            ?: (PropertyFactory.getString(ALL_WORKERS, DOMAIN)
                ?: taskToDomain[taskType])
    }

    companion object {
        private const val DOMAIN = "domain"
        private const val OVERRIDE_DISCOVERY = "pollOutOfDiscovery"
        private const val ALL_WORKERS = "all"
        private const val LEASE_EXTEND_RETRY_COUNT = 3
        private const val LEASE_EXTEND_DURATION_FACTOR = 0.8
    }
}

suspend fun TaskResult.logStackTrace(t: Throwable) {
    withContext(Dispatchers.IO) {
        log(t.stackTraceToString())
    }
}

typealias TaskWithResult = Pair<Task, TaskResult>