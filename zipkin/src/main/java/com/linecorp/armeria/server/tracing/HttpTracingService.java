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

package com.linecorp.armeria.server.tracing;

import java.net.SocketAddress;
import java.util.function.Function;

import com.linecorp.armeria.common.HttpRequest;
import com.linecorp.armeria.common.HttpResponse;
import com.linecorp.armeria.common.RpcRequest;
import com.linecorp.armeria.common.logging.RequestLog;
import com.linecorp.armeria.common.logging.RequestLogAvailability;
import com.linecorp.armeria.internal.tracing.AsciiStringKeyFactory;
import com.linecorp.armeria.internal.tracing.SpanTags;
import com.linecorp.armeria.internal.tracing.TracingRequest;
import com.linecorp.armeria.internal.tracing.TracingResponse;
import com.linecorp.armeria.server.Service;
import com.linecorp.armeria.server.ServiceRequestContext;
import com.linecorp.armeria.server.SimpleDecoratingService;

import brave.Span;
import brave.Tracer;
import brave.Tracer.SpanInScope;
import brave.Tracing;
import brave.http.HttpAdapter;
import brave.http.HttpServerAdapter;
import brave.http.HttpServerHandler;
import brave.http.HttpServerParser;
import brave.http.HttpTracing;
import brave.propagation.TraceContext;
import io.netty.util.AsciiString;
import io.netty.util.concurrent.FastThreadLocal;
import zipkin2.Endpoint.Builder;

/**
 * Decorates a {@link Service} to trace inbound {@link HttpRequest}s using
 * <a href="http://zipkin.io/">Zipkin</a>.
 *
 * <p>This decorator retrieves trace data from HTTP headers. The specifications of header names and its values
 * correspond to <a href="http://zipkin.io/">Zipkin</a>.
 */
public class HttpTracingService extends SimpleDecoratingService<HttpRequest, HttpResponse> {

    private static final FastThreadLocal<SpanInScope> SPAN_IN_THREAD = new FastThreadLocal<>();
    private final HttpServerHandler<TracingRequest, TracingResponse> httpServerHandler;

    /**
     * Creates a new tracing {@link Service} decorator using the specified {@link HttpTracing} instance.
     */
    public static Function<Service<HttpRequest, HttpResponse>, HttpTracingService>
    newDecorator(Tracing tracing) {
        return service -> new HttpTracingService(service, tracing);
    }

    private final Tracer tracer;
    private final TraceContext.Extractor<TracingRequest> extractor;

    private final static class ServerParser extends HttpServerParser {
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

    private static final class ServerAdapter extends HttpServerAdapter<TracingRequest, TracingResponse> {

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
        public boolean parseClientAddress(TracingRequest tracingRequest, Builder builder) {
            final SocketAddress raddr = tracingRequest.requestLog().context().remoteAddress();
            if (raddr != null) {
                return true;
            } else {
                return super.parseClientAddress(tracingRequest, builder);
            }
        }
    }

    /**
     * Creates a new instance.
     */
    public HttpTracingService(Service<HttpRequest, HttpResponse> delegate, Tracing tracing) {
        super(delegate);
        HttpTracing httpTracing = HttpTracing.newBuilder(tracing)
                                             .serverParser(new ServerParser()).build();
        tracer = tracing.tracer();
        extractor = httpTracing.tracing().propagationFactory().create(AsciiStringKeyFactory.INSTANCE)
                               .extractor((req, name) -> req.httpRequest().headers().get(name));
        httpServerHandler = HttpServerHandler.create(httpTracing, new ServerAdapter());
    }

    @Override
    public HttpResponse serve(ServiceRequestContext ctx, HttpRequest req) throws Exception {
        Span span = httpServerHandler.handleReceive(extractor, new TracingRequest(ctx.log(), req));
        if (span.isNoop()) {
            return delegate().serve(ctx, req);
        }

        SpanTags.customTag(span, ctx.log());
        SPAN_IN_THREAD.set(tracer.withSpanInScope(span));
        ctx.log().addListener(log -> {
            final SpanInScope spanInScope = SPAN_IN_THREAD.get();
            httpServerHandler.handleSend(new TracingResponse(log), log.responseCause(), span);
            spanInScope.close();
        }, RequestLogAvailability.COMPLETE);
        return delegate().serve(ctx, req);
    }
}
