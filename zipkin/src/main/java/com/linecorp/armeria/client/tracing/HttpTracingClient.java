/*
 * Copyright 2016 LINE Corporation
 *
 * LINE Corporation licenses this file to you under the Apache License,
 * version 2.0 (the "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at:
 *
 *   https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations
 * under the License.
 */

package com.linecorp.armeria.client.tracing;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.util.function.Function;

import javax.annotation.Nullable;

import com.linecorp.armeria.client.Client;
import com.linecorp.armeria.client.ClientRequestContext;
import com.linecorp.armeria.client.SimpleDecoratingClient;
import com.linecorp.armeria.common.HttpRequest;
import com.linecorp.armeria.common.HttpResponse;
import com.linecorp.armeria.common.RpcRequest;
import com.linecorp.armeria.common.logging.RequestLog;
import com.linecorp.armeria.common.logging.RequestLogAvailability;
import com.linecorp.armeria.internal.tracing.AsciiStringKeyFactory;
import com.linecorp.armeria.internal.tracing.TracingRequest;
import com.linecorp.armeria.internal.tracing.TracingResponse;

import brave.Span;
import brave.Span.Kind;
import brave.Tracer;
import brave.Tracer.SpanInScope;
import brave.Tracing;
import brave.http.HttpAdapter;
import brave.http.HttpClientAdapter;
import brave.http.HttpClientHandler;
import brave.http.HttpClientParser;
import brave.http.HttpTracing;
import brave.propagation.TraceContext;
import io.netty.util.AsciiString;
import io.netty.util.concurrent.FastThreadLocal;
import zipkin2.Endpoint;
import zipkin2.Endpoint.Builder;

/**
 * Decorates a {@link Client} to trace outbound {@link HttpRequest}s using
 * <a href="http://zipkin.io/">Zipkin</a>.
 *
 * <p>This decorator puts trace data into HTTP headers. The specifications of header names and its values
 * correspond to <a href="http://zipkin.io/">Zipkin</a>.
 */
public class HttpTracingClient extends SimpleDecoratingClient<HttpRequest, HttpResponse> {

    private static final FastThreadLocal<SpanInScope> SPAN_IN_THREAD = new FastThreadLocal<>();

    private static final class ClientAdapter extends HttpClientAdapter<TracingRequest, TracingResponse> {

        @Override
        public String method(TracingRequest request) {
            return request.httpRequest().method().name();
        }

        @Override
        public String url(TracingRequest request) {
            final RequestLog log = request.requestLog();
            final StringBuilder uriBuilder = new StringBuilder()
                    .append(log.scheme().uriText())
                    .append("://")
                    .append(log.authority())
                    .append(log.path());
            if (log.query() != null) {
                uriBuilder.append('?').append(log.query());
            }
            return uriBuilder.append('?').append(log.query()).toString();
        }

        @Override
        public String requestHeader(TracingRequest request, String name) {
            return request.httpRequest().headers().get(AsciiString.of(name));
        }

        @Override
        public Integer statusCode(TracingResponse response) {
            return response.requestLog().status().code();
        }

        @Override
        public boolean parseServerAddress(TracingRequest tracingRequest, Builder builder) {
            //TBD
            return true;
        }
    }

    /**
     * Creates a new tracing {@link Client} decorator using the specified {@link Tracing} instance.
     */
    public static Function<Client<HttpRequest, HttpResponse>, HttpTracingClient> newDecorator(Tracing tracing) {
        return newDecorator(tracing, null);
    }

    /**
     * Creates a new tracing {@link Client} decorator using the specified {@link Tracing} instance
     * and remote service name.
     */
    public static Function<Client<HttpRequest, HttpResponse>, HttpTracingClient> newDecorator(
            Tracing tracing,
            @Nullable String remoteServiceName) {
        return delegate -> new HttpTracingClient(delegate, tracing, remoteServiceName);
    }

    private final Tracer tracer;
    private final TraceContext.Injector<TracingRequest> injector;
    @Nullable
    private final String remoteServiceName;
    private final HttpClientHandler<TracingRequest, TracingResponse> httpClientHandler;

    private final static class ClientParser extends HttpClientParser {
        @Override
        protected <Req> String spanName(HttpAdapter<Req, ?> adapter, Req req) {
            TracingRequest tr = (TracingRequest) req;
            Object requestContent = tr.requestLog().requestContent();
            if (requestContent instanceof RpcRequest) {
                return ((RpcRequest) requestContent).method();
            } else {
                return super.spanName(adapter, req);
            }
        }
    }

    /**
     * Creates a new instance.
     */
    protected HttpTracingClient(Client<HttpRequest, HttpResponse> delegate, Tracing tracing,
                                @Nullable String remoteServiceName) {
        super(delegate);
        HttpTracing httpTracing = HttpTracing.newBuilder(tracing)
                                             .clientParser(new ClientParser()).build();
        tracer = tracing.tracer();
        injector = tracing.propagationFactory().create(AsciiStringKeyFactory.INSTANCE)
                          .injector((req, key, value) -> req.httpRequest().headers().set(key, value));
        httpClientHandler = HttpClientHandler.create(httpTracing, new ClientAdapter());
        this.remoteServiceName = remoteServiceName;
    }

    @Override
    public HttpResponse execute(ClientRequestContext ctx, HttpRequest request) throws Exception {
        TracingRequest req = new TracingRequest(ctx.log(), request);
        final Span span = httpClientHandler.handleSend(injector, req);
        setRemoteEndpoint(span, ctx.log());
        if (span.isNoop()) {
            return delegate().execute(ctx, request);
        }
        span.kind(Kind.CLIENT).start();
        SPAN_IN_THREAD.set(tracer.withSpanInScope(span));
        ctx.log().addListener(log -> {
            final SpanInScope spanInScope = SPAN_IN_THREAD.get();
            httpClientHandler.handleReceive(new TracingResponse(log),
                                            log.responseCause(), span);
            spanInScope.close();
        }, RequestLogAvailability.COMPLETE);
        return delegate().execute(ctx, request);
    }

    private void setRemoteEndpoint(Span span, RequestLog log) {
        final SocketAddress remoteAddress = log.context().remoteAddress();
        final InetAddress address;
        if (remoteAddress instanceof InetSocketAddress) {
            address = ((InetSocketAddress) remoteAddress).getAddress();
        } else {
            address = null;
        }

        final String remoteServiceName;
        if (this.remoteServiceName != null) {
            remoteServiceName = this.remoteServiceName;
        } else {
            final String authority = log.requestHeaders().authority();
            if (!"?".equals(authority)) {
                remoteServiceName = authority;
            } else if (address != null) {
                remoteServiceName = String.valueOf(remoteAddress);
            } else {
                remoteServiceName = null;
            }
        }

        if (remoteServiceName == null && address == null) {
            return;
        }

        span.remoteEndpoint(Endpoint.newBuilder().serviceName(remoteServiceName).ip(address).build());
    }
}
