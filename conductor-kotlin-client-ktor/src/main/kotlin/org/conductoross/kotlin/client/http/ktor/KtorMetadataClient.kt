package org.conductoross.kotlin.client.http.ktor

import org.conductoross.kotlin.client.http.MetadataClient
import com.netflix.conductor.common.metadata.tasks.TaskDef
import com.netflix.conductor.common.metadata.workflow.WorkflowDef
import io.ktor.client.*
import io.ktor.client.call.*
import io.ktor.client.request.*
import io.ktor.http.*

class KtorMetadataClient(rootURI: String, httpClient: HttpClient): MetadataClient, KtorBaseClient(rootURI, httpClient) {

    override suspend fun registerWorkflowDef(workflowDef: WorkflowDef) {
        httpClient.post {
            url("$rootURI/metadata/workflow")
            setBody(workflowDef)
            contentType(ContentType.Application.Json)
        }
    }

    override suspend fun validateWorkflowDef(workflowDef: WorkflowDef) {
        httpClient.post {
            url("$rootURI/metadata/workflow/validate")
            setBody(workflowDef)
            contentType(ContentType.Application.Json)
        }
    }

    override suspend fun updateWorkflowDefs(workflowDefs: List<WorkflowDef>) {
        httpClient.put {
            url("$rootURI/metadata/workflow")
            setBody(workflowDefs)
            contentType(ContentType.Application.Json)
        }
    }

    override suspend fun getWorkflowDef(name: String, version: Int?): WorkflowDef {
        require(name.isNotBlank()) { "name cannot be blank" }
        val response = httpClient.get {
            url("$rootURI/metadata/workflow/$name")
            parameter("version", version)
        }
        return response.body()
    }

    override suspend fun unregisterWorkflowDef(name: String, version: Int) {
        require(name.isNotBlank()) { "name cannot be blank" }
        httpClient.delete() {
            url("$rootURI/metadata/workflow/$name/$version")
        }
    }

    override suspend fun registerTaskDefs(taskDefs: List<TaskDef>) {
        httpClient.post {
            url("$rootURI/metadata/taskdefs")
            setBody(taskDefs)
            contentType(ContentType.Application.Json)
        }
    }

    override suspend fun updateTaskDef(taskDef: TaskDef) {
        httpClient.put {
            url("$rootURI/metadata/taskdefs")
            setBody(taskDef)
            contentType(ContentType.Application.Json)
        }
    }

    override suspend fun getTaskDef(taskType: String): TaskDef {
        require(taskType.isNotBlank()) { "Task type cannot be blank" }
        val response = httpClient.get {
            url("$rootURI/metadata/taskdefs/${taskType}")
        }
        return response.body()
    }

    override suspend fun unregisterTaskDef(taskType: String) {
        require(taskType.isNotBlank()) { "Task type cannot be blank" }
        httpClient.delete() {
            url("$rootURI/metadata/taskdefs/${taskType}")
        }
    }
}