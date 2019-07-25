/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.ratis.client.impl;

import java.io.IOException;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.Semaphore;
import java.util.function.Function;
import java.util.function.LongFunction;

import org.apache.ratis.client.RaftClientConfigKeys;
import org.apache.ratis.client.impl.RaftClientImpl.PendingClientRequest;
import org.apache.ratis.conf.RaftProperties;
import org.apache.ratis.proto.RaftProtos.RaftClientRequestProto.TypeCase;
import org.apache.ratis.proto.RaftProtos.SlidingWindowEntry;
import org.apache.ratis.protocol.GroupMismatchException;
import org.apache.ratis.protocol.Message;
import org.apache.ratis.protocol.NotLeaderException;
import org.apache.ratis.protocol.RaftClientReply;
import org.apache.ratis.protocol.RaftClientRequest;
import org.apache.ratis.protocol.RaftException;
import org.apache.ratis.protocol.RaftPeerId;
import org.apache.ratis.retry.RetryPolicies;
import org.apache.ratis.retry.RetryPolicy;
import org.apache.ratis.tracing.TracingUtil;
import org.apache.ratis.util.IOUtils;
import org.apache.ratis.util.JavaUtils;
import org.apache.ratis.util.Preconditions;
import org.apache.ratis.util.ProtoUtils;
import org.apache.ratis.util.SlidingWindow;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.opentracing.Span;

/** Send ordered asynchronous requests to a raft service. */
class OrderedAsync {
  static final Logger LOG = LoggerFactory.getLogger(OrderedAsync.class);

  static class PendingOrderedRequest extends PendingClientRequest
      implements SlidingWindow.ClientSideRequest<RaftClientReply> {
    private final Function<SlidingWindowEntry, RaftClientRequest> requestConstructor;
    private final long seqNum;
    private volatile boolean isFirst = false;
    private volatile RaftClientRequest request;

    PendingOrderedRequest(long seqNum, Function<SlidingWindowEntry, RaftClientRequest> requestConstructor) {
      this.seqNum = seqNum;
      this.requestConstructor = requestConstructor;
    }

    @Override
    RaftClientRequest newRequestImpl() {
      request = requestConstructor.apply(ProtoUtils.toSlidingWindowEntry(seqNum, isFirst));
      return request;
    }

    RaftClientRequest getRequest() {
      return request;
    }

    @Override
    public void setFirstRequest() {
      isFirst = true;
    }

    @Override
    public long getSeqNum() {
      return seqNum;
    }

    @Override
    public boolean hasReply() {
      return getReplyFuture().isDone();
    }

    @Override
    public void setReply(RaftClientReply reply) {
      getReplyFuture().complete(reply);
    }

    @Override
    public void fail(Throwable e) {
      getReplyFuture().completeExceptionally(e);
    }

    @Override
    public String toString() {
      return "[seq=" + getSeqNum() + "]";
    }
  }

  private final RaftClientImpl client;
  /** Map: id -> {@link SlidingWindow}, in order to support async calls to the Raft service or individual servers. */
  private final ConcurrentMap<String, SlidingWindow.Client<PendingOrderedRequest, RaftClientReply>> slidingWindows
      = new ConcurrentHashMap<>();
  private final Semaphore requestSemaphore;

  OrderedAsync(RaftClientImpl client, RaftProperties properties) {
    this.client = Objects.requireNonNull(client, "client == null");
    this.requestSemaphore = new Semaphore(RaftClientConfigKeys.Async.maxOutstandingRequests(properties));
  }

  private void resetSlidingWindow(RaftClientRequest request) {
    getSlidingWindow(request).resetFirstSeqNum();
  }

  private SlidingWindow.Client<PendingOrderedRequest, RaftClientReply> getSlidingWindow(RaftClientRequest request) {
    return getSlidingWindow(request.is(TypeCase.STALEREAD) ? request.getServerId() : null);
  }

  private SlidingWindow.Client<PendingOrderedRequest, RaftClientReply> getSlidingWindow(RaftPeerId target) {
    final String id = target != null ? target.toString() : "RAFT";
    return slidingWindows.computeIfAbsent(id, key -> new SlidingWindow.Client<>(client.getId() + "->" + key));
  }

  private void failAllAsyncRequests(RaftClientRequest request, Throwable t) {
    getSlidingWindow(request).fail(request.getSlidingWindowEntry().getSeqNum(), t);
  }

  private void handleAsyncRetryFailure(RaftClientRequest request, int attemptCount, Throwable throwable) {
    failAllAsyncRequests(request, client.noMoreRetries(request, attemptCount, throwable));
  }

  CompletableFuture<RaftClientReply> send(RaftClientRequest.Type type, Message message, RaftPeerId server) {
    if (!type.is(TypeCase.WATCH)) {
      Objects.requireNonNull(message, "message == null");
    }
    try {
      requestSemaphore.acquire();
    } catch (InterruptedException e) {
      return JavaUtils.completeExceptionally(IOUtils.toInterruptedIOException(
          "Interrupted when sending " + type + ", message=" + message, e));
    }

    final long callId = RaftClientImpl.nextCallId();
    final Span span = TracingUtil.activeSpan();
    final LongFunction<PendingOrderedRequest> constructor = seqNum -> new PendingOrderedRequest(seqNum,
        slidingWindowEntry -> client.newRaftClientRequest(server, callId, message, type, slidingWindowEntry, span));
    return getSlidingWindow(server).submitNewRequest(constructor, this::sendRequestWithRetry
    ).getReplyFuture(
    ).thenApply(reply -> RaftClientImpl.handleRaftException(reply, CompletionException::new)
    ).whenComplete((r, e) -> requestSemaphore.release());
  }

  private void sendRequestWithRetry(PendingOrderedRequest pending) {
    final RetryPolicy retryPolicy = client.getRetryPolicy();
    final CompletableFuture<RaftClientReply> f = pending.getReplyFuture();
    if (f.isDone()) {
      return;
    }

    RaftClientRequest request = pending.newRequestImpl();
    sendRequest(pending).thenAccept(reply -> {
      if (f.isDone()) {
        return;
      }
      if (reply == null) {
        scheduleWithTimeout(pending, request, retryPolicy);
      } else {
        f.complete(reply);
      }
    }).exceptionally(e -> {
      e = JavaUtils.unwrapCompletionException(e);
      if (e instanceof NotLeaderException) {
        RetryPolicy noLeaderRetry = ((NotLeaderException) e).getSuggestedLeader() != null ?
            RetryPolicies.retryForeverNoSleep() : retryPolicy;
        scheduleWithTimeout(pending, request, noLeaderRetry);
        return null;
      }
      f.completeExceptionally(e);
      return null;
    });
  }

  private void scheduleWithTimeout(PendingOrderedRequest pending, RaftClientRequest request, RetryPolicy retryPolicy) {
    final int attempt = pending.getAttemptCount();
    LOG.debug("schedule* attempt #{} with policy {} for {}", attempt, retryPolicy, request);
    client.getScheduler().onTimeout(retryPolicy.getSleepTime(attempt, request),
        () -> getSlidingWindow(request).retry(pending, this::sendRequestWithRetry),
        LOG, () -> "Failed* to retry " + request);
  }

  private CompletableFuture<RaftClientReply> sendRequest(PendingOrderedRequest pending) {
    final RetryPolicy retryPolicy = client.getRetryPolicy();
    final CompletableFuture<RaftClientReply> f;
    final RaftClientRequest request;
    if (getSlidingWindow((RaftPeerId) null).isFirst(pending.getSeqNum())) {
      pending.setFirstRequest();
    }
    request = pending.newRequest();
    LOG.debug("{}: send* {}", client.getId(), request);
    f = client.getClientRpc().sendRequestAsync(request);
    int attemptCount = pending.getAttemptCount();
    return f.thenApply(reply -> {
      LOG.debug("{}: receive* {}", client.getId(), reply);
      final RaftException replyException = reply != null? reply.getException(): null;
      reply = client.handleLeaderException(request, reply, this::resetSlidingWindow);
      if (reply != null) {
        getSlidingWindow(request).receiveReply(
            request.getSlidingWindowEntry().getSeqNum(), reply, this::sendRequestWithRetry);
      } else if (!retryPolicy.shouldRetry(attemptCount, request)) {
        handleAsyncRetryFailure(request, attemptCount, replyException);
      }
      return reply;
    }).exceptionally(e -> {
      if (LOG.isTraceEnabled()) {
        LOG.trace(client.getId() + ": Failed* " + request, e);
      } else {
        LOG.debug("{}: Failed* {} with {}", client.getId(), request, e);
      }
      e = JavaUtils.unwrapCompletionException(e);
      if (e instanceof IOException && !(e instanceof GroupMismatchException)) {
        if (!retryPolicy.shouldRetry(attemptCount, request)) {
          handleAsyncRetryFailure(request, attemptCount, e);
        } else {
          if (e instanceof NotLeaderException) {
            NotLeaderException nle = (NotLeaderException)e;
            client.handleNotLeaderException(request, nle, this::resetSlidingWindow);
          } else {
            client.handleIOException(request, (IOException) e, null, this::resetSlidingWindow);
          }
        }
        if (e instanceof NotLeaderException) {
          throw new CompletionException(e);
        }
        return null;
      }
      failAllAsyncRequests(request, e);
      return null;
    });
  }

  void assertRequestSemaphore(int expectedAvailablePermits, int expectedQueueLength) {
    Preconditions.assertTrue(requestSemaphore.availablePermits() == expectedAvailablePermits);
    Preconditions.assertTrue(requestSemaphore.getQueueLength() == expectedQueueLength);
  }
}
