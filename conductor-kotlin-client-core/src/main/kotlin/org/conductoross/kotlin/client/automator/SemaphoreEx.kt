package org.conductoross.kotlin.client.automator

import kotlinx.coroutines.sync.Semaphore

fun Semaphore.release(numSlots: Int) {
    repeat(numSlots) { this.release() }
}

fun Semaphore.tryAcquire(numSlots: Int): Boolean {
    if (this.availablePermits < numSlots) return false
    var acquireCount = 0
    repeat(numSlots) {
        if (this.tryAcquire()) acquireCount++ else {
            release(acquireCount)
            return false
        }
    }
    return true
}