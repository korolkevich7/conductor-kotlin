package org.conductoross.client.kotlin.http.ktor

import com.fasterxml.jackson.module.kotlin.jacksonObjectMapper
import io.ktor.client.*
import io.ktor.client.engine.*
import io.ktor.http.*


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