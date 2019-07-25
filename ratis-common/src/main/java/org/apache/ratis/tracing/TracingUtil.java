/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.ratis.tracing;

import java.lang.reflect.Proxy;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.CompletableFuture;
import java.util.function.BiConsumer;
import java.util.function.Function;
import java.util.function.Supplier;

import org.apache.ratis.thirdparty.com.google.protobuf.ByteString;
import org.apache.ratis.util.function.FunctionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.jaegertracing.Configuration;
import io.opentracing.Scope;
import io.opentracing.Span;
import io.opentracing.SpanContext;
import io.opentracing.Tracer;
import io.opentracing.util.GlobalTracer;

/**
 * Utility class to collect all the tracing helper methods.
 */
public final class TracingUtil {

  private static final Logger LOG = LoggerFactory.getLogger(TracingUtil.class);

  private static final Scope NOOP_SCOPE = () -> {};

  private TracingUtil() {
  }

  public static <T> CompletableFuture<T> thenActivateSpan(
      CompletableFuture<T> future, Span span) {
    return span != null
      ? future.thenApply(FunctionUtils.consumerAsIdentity(__ -> GlobalTracer.get().scopeManager().activate(span)))
      : future;
  }

  public static Scope activate(Span span) {
    return span != null ? GlobalTracer.get().scopeManager().activate(span) : NOOP_SCOPE;
  }

  public static <O> O withActiveSpan(Supplier<O> op, Span span) {
    try (Scope ignored = activate(span)) {
      return op.get();
    }
  }

  public static <O> void activateSpan(Runnable op, Span span) {
    try (Scope ignored = activate(span)) {
      op.run();
    }
  }

  public static <T> Function<T, T> spanFinisher(Span span, Logger logger) {
    return span != null
      ? FunctionUtils.consumerAsIdentity(__ -> finish(span, logger))
      : Function.identity();
  }

  public static <R, T extends Throwable> BiConsumer<R, T> finishSpan(Span span, Logger logger) {
    return (r, e) -> finish(span, logger);
  }

  public static void finish(Span span) {
    finish(span, null);
  }

  public static void finish(Span span, Logger logger) {
    if (span != null) {
      if (logger != null) {
        logger.info("Finishing span {}", span);
      }
      span.finish();
    }
  }

  /**
   * Initialize the tracing with the given service name.
   *
   * @param serviceName
   */
  public static Tracer initTracing(String serviceName) {
    GlobalTracer.registerIfAbsent(() -> Configuration.fromEnv(serviceName)
        .getTracerBuilder()
        .registerExtractor(StringCodec.FORMAT, new StringCodec())
        .registerInjector(StringCodec.FORMAT, new StringCodec())
        .build());
    return GlobalTracer.get();
  }

  /**
   * Export the active tracing span as a string.
   *
   * @return encoded tracing context.
   */
  public static ByteString exportCurrentSpan() {
    Tracer tracer = GlobalTracer.get();
    Span span = tracer.activeSpan();
    return exportSpan(span);
  }

  public static ByteString exportSpan(Span span) {
    if (span != null) {
      Tracer tracer = GlobalTracer.get();
      StringBuilder builder = new StringBuilder();
      tracer.inject(span.context(), StringCodec.FORMAT, builder);
      LOG.info("Exporting span {}", span);
      return ByteString.copyFromUtf8(builder.toString());
    }
    LOG.warn("No span to export", new Exception());
    return ByteString.EMPTY;
  }

  /**
   * Create and start a new span, using the imported span as the parent.
   *
   * @param name name of the newly created span
   * @return OpenTracing span
   */
  public static Span startSpan(String name, ByteString trace) {
    Tracer tracer = GlobalTracer.get();
    SpanContext parentSpan = extract(trace);
    Tracer.SpanBuilder spanBuilder = tracer.buildSpan(name);
    if (parentSpan != null) {
      spanBuilder = spanBuilder.asChildOf(parentSpan);
    }
    return spanBuilder.start();
  }

  public static Span startIfTracePresent(String name, ByteString trace) {
    SpanContext parentSpan = extract(trace);
    if (parentSpan != null) {
      Tracer tracer = GlobalTracer.get();
      return tracer.buildSpan(name).asChildOf(parentSpan).start();
    }
    return null;
  }

  public static SpanContext extract(ByteString trace) {
    String encodedParent = new String(trace.toByteArray(), StandardCharsets.UTF_8);
    if (encodedParent.length() > 0) {
      Tracer tracer = GlobalTracer.get();
      StringBuilder builder = new StringBuilder(encodedParent);
      SpanContext parentSpan = tracer.extract(StringCodec.FORMAT, builder);
      LOG.info("Extracted span context {}", parentSpan);
      return parentSpan;
    }
    return null;
  }

  public static Span startSpan(String name) {
    Tracer tracer = GlobalTracer.get();
    return tracer.buildSpan(name).start();
  }

  /**
   * Creates a proxy of the implementation and trace all the method calls.
   *
   * @param delegate the original class instance
   * @param interfce the interface which should be implemented by the proxy
   * @return A new interface which implements interfce but delegate all the
   * calls to the delegate and also enables tracing.
   */
  public static <T> T createProxy(T delegate, Class<T> interfce) {
    Class<?> aClass = delegate.getClass();
    return interfce.cast(Proxy.newProxyInstance(aClass.getClassLoader(),
        new Class<?>[] {interfce},
        new TraceAllMethod<>(delegate, interfce.getSimpleName())));
  }

  public static void log(String event) {
    Span span = activeSpan();
    if (span != null) {
      span.log(event);
    }
  }

  public static Span activeSpan() {
    return GlobalTracer.get().activeSpan();
  }

}
