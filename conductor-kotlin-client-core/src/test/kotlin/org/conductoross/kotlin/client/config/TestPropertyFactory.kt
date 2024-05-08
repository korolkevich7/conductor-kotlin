package org.conductoross.kotlin.client.config

import org.conductoross.kotlin.client.config.PropertyFactory.Companion.getBoolean
import org.conductoross.kotlin.client.config.PropertyFactory.Companion.getInteger
import org.conductoross.kotlin.client.config.PropertyFactory.Companion.getString
import org.conductoross.kotlin.client.worker.Worker.Companion.create
import com.netflix.conductor.common.metadata.tasks.TaskResult
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertNull
import kotlin.test.assertFalse
import kotlin.test.assertNotNull
import kotlin.test.assertTrue

class TestPropertyFactory {
    @Test
    fun testIdentity() {
        val worker = create("Test2") { task -> TaskResult(task) }
        assertNotNull(worker.identity)
        val paused = worker.paused()
        assertFalse(paused, "Paused? $paused")
    }

    @Test
    fun test() {
        val property = getInteger("workerB", "pollingInterval") ?: 100
        assertEquals( 2, property.toLong(), "got: $property")
        assertEquals(
            100, getInteger("workerB", "propWithoutValue") ?: 100
        )
        assertFalse(getBoolean("workerB", "paused") ?: true) // Global value set to 'false'
        assertTrue(getBoolean("workerA", "paused") ?: false) // WorkerA value set to 'true'
        assertEquals(
            42,
            getInteger("workerA", "batchSize") ?: 42
        ) // No global value set, so will return the default value
        // supplied
        assertEquals(
            84,
            getInteger("workerB", "batchSize") ?: 42
        ) // WorkerB's value set to 84
        assertEquals("domainA", getString("workerA", "domain"))
        assertEquals("domainB", getString("workerB", "domain"))
        assertNull(getString("workerC", "domain")) // Non Existent
    }

    @Test
    fun testProperty() {
        val worker = create("Test") { task -> TaskResult(task) }
        val paused = worker.paused()
        assertTrue(paused, "Paused? $paused")
    }
}
