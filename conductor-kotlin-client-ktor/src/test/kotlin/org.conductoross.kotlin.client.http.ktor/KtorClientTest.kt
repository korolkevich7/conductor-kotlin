package org.conductoross.kotlin.client.http.ktor

import com.fasterxml.jackson.module.kotlin.jacksonObjectMapper
import io.ktor.client.*
import io.ktor.client.engine.*
import io.ktor.http.*
import org.conductoross.kotlin.client.http.ktor.configureObjectMapper
import org.conductoross.kotlin.client.http.ktor.defaultHttpClient


const val ROOT_URL = "http://localhost/dummyroot"
abstract class KtorClientTest {

//    protected val requestHandler: JerseyClientRequestHandler = mock()
    protected val objectMapper = jacksonObjectMapper()

    init {
        objectMapper.configureObjectMapper()
    }

    fun httpClient(engine: HttpClientEngine): HttpClient = defaultHttpClient(engine)

    fun headerJson() = headersOf(HttpHeaders.ContentType, "application/json")
}