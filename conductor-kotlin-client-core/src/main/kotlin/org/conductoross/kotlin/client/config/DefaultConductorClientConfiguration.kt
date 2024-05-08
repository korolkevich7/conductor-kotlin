package org.conductoross.kotlin.client.config

/**
 * A default implementation of [ConductorClientConfiguration] where external payload storage
 * is disabled.
 */
object DefaultConductorClientConfiguration :
    _root_ide_package_.org.conductoross.kotlin.client.config.ConductorClientConfiguration {

    override fun getWorkflowInputPayloadThresholdKB(): Int = 5120

    override fun getWorkflowInputMaxPayloadThresholdKB(): Int = 10240

    override fun getTaskOutputPayloadThresholdKB(): Int = 3072

    override fun getTaskOutputMaxPayloadThresholdKB(): Int = 10240

    override fun isExternalPayloadStorageEnabled(): Boolean = false
}
