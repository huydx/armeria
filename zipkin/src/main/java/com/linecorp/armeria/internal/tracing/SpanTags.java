/*
 * Copyright 2017 LINE Corporation
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

package com.linecorp.armeria.internal.tracing;

import java.net.SocketAddress;

import com.linecorp.armeria.common.RpcRequest;
import com.linecorp.armeria.common.logging.RequestLog;

import brave.Span;

/**
 * Adds standard Zipkin tags to a span with the information in a {@link RequestLog}.
 */
public final class SpanTags {

    /**
     * Add Armeria specific naming and tagging.
     */
    public static void customTag(Span span, RequestLog log) {
        final String host = log.requestHeaders().authority();
        assert host != null;
        span.tag("http.host", host);

        final SocketAddress raddr = log.context().remoteAddress();
        if (raddr != null) {
            span.tag("address.remote", raddr.toString());
        }
        final SocketAddress laddr = log.context().localAddress();
        if (laddr != null) {
            span.tag("address.local", laddr.toString());
        }
    }

    private SpanTags() {}
}
