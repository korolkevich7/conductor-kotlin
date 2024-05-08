package com.netflix.spectator.api

import com.netflix.spectator.api.Timer
import kotlin.time.Duration
import kotlin.time.toJavaDuration

fun Timer.record(amount: Duration) = record(amount.toJavaDuration())

