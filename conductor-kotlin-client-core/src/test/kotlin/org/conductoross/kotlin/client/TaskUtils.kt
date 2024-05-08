package org.conductoross.kotlin.client

import com.netflix.conductor.common.metadata.tasks.Task
import java.util.*

fun task(taskType: String) = Task().apply {
    this.taskId = UUID.randomUUID().toString()
    this.status = Task.Status.IN_PROGRESS
    this.taskDefName = taskType
}