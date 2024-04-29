package org.conductoross.client.kotlin.automator

import com.netflix.conductor.common.metadata.tasks.Task
import io.github.oshai.kotlinlogging.KotlinLogging
import kotlin.time.Duration.Companion.milliseconds
import kotlin.time.ExperimentalTime
import kotlinx.coroutines.CoroutineDispatcher
import kotlinx.coroutines.CoroutineName
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.DelicateCoroutinesApi
import kotlinx.coroutines.ExperimentalCoroutinesApi
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.channels.BufferOverflow
import kotlinx.coroutines.channels.ReceiveChannel
import kotlinx.coroutines.channels.produce
import kotlinx.coroutines.delay
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.asFlow
import kotlinx.coroutines.flow.buffer
import kotlinx.coroutines.flow.catch
import kotlinx.coroutines.flow.collect
import kotlinx.coroutines.flow.emitAll
import kotlinx.coroutines.flow.flow
import kotlinx.coroutines.flow.flowOn
import kotlinx.coroutines.flow.map
import kotlinx.coroutines.flow.onEach
import kotlinx.coroutines.launch
import org.conductoross.client.kotlin.exception.ConductorTimeoutClientException
import org.conductoross.client.kotlin.worker.Worker

private val logger = KotlinLogging.logger {}

internal fun startWorkerWithChannel(worker: Worker, workersDispatcher: CoroutineDispatcher, taskPollExecutor: TaskPollExecutor) {
    val taskDispatcher = worker.dispatcher(workersDispatcher)
    val workerScope = CoroutineScope(taskDispatcher + SupervisorJob() + CoroutineName("Task ${worker.taskDefName} context"))

    val taskChannel: ReceiveChannel<Task> = workerScope.receiveChannel(worker, taskPollExecutor)

    workerScope.launch {
        for (task in taskChannel) {
            runCatching { taskPollExecutor.processTask(worker, task) }
                .onSuccess { taskPollExecutor.updateTaskResult(it.first, it.second, worker) }
                .onFailure { handleProcessException(it) }
        }
    }
}

@OptIn(ExperimentalCoroutinesApi::class, DelicateCoroutinesApi::class)
private fun CoroutineScope.receiveChannel(worker: Worker, taskPollExecutor: TaskPollExecutor) = produce(capacity = worker.batchPollCount) {
    while (this.isClosedForSend.not()) {
        val pollingResult = runCatching { taskPollExecutor.poll(worker) }
        pollingResult
            .onFailure {
                logger.error(it.cause) { "Failed to poll for tasks for worker ${worker.taskDefName} with error ${it.message}" }
            }.onSuccess { tasks ->
                tasks.forEach { send(it) }
            }
        delay(worker.pollingInterval.milliseconds)
    }
}


internal fun startWorkerWithFlow(worker: Worker, workersDispatcher: CoroutineDispatcher, taskPollExecutor: TaskPollExecutor) {
    val taskDispatcher = worker.dispatcher(workersDispatcher)
    val workerScope = CoroutineScope(
        taskDispatcher +
                SupervisorJob() +
                CoroutineName("Task ${worker.taskDefName} context")
    )
    val flow = workerScope.taskFlow(taskPollExecutor, worker)
        .map {
            taskPollExecutor.processTask(worker, it)
        }
        .catch { handleProcessException(it) }
        .onEach { taskPollExecutor.updateTaskResult(it.first, it.second, worker) }
        .flowOn(taskDispatcher)

    CoroutineScope(SupervisorJob()).launch(workersDispatcher) {
        flow.collect()
    }
}

@OptIn(ExperimentalTime::class)
internal fun CoroutineScope.taskFlow(executor: TaskPollExecutor, worker: Worker): Flow<Task> = flow {
    timerExact(interval = worker.pollingInterval.milliseconds) {
        val pollingResult = runCatching { executor.poll(worker) }
        pollingResult
            .onFailure {
                logger.error(it.cause) { "Failed to poll for tasks for worker ${worker.taskDefName} with error ${it.message}" }
            }.onSuccess { tasks ->
                emitAll(tasks.asFlow())
            }
    }
}.buffer(worker.batchPollCount, onBufferOverflow = BufferOverflow.SUSPEND)

@OptIn(ExperimentalCoroutinesApi::class)
internal fun Worker.dispatcher(coroutineDispatcher: CoroutineDispatcher): CoroutineDispatcher =
    this.limitedParallelism?.let { coroutineDispatcher.limitedParallelism(it) } ?: coroutineDispatcher

internal fun handleProcessException(t: Throwable) {
    when (t) {
        is ConductorTimeoutClientException -> logger.warn { t.message }
        else -> logger.error { t.message }
    }
}
