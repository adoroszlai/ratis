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
package org.apache.ratis.protocol;

import static org.apache.ratis.proto.RaftProtos.RaftClientRequestProto.TypeCase.READ;
import static org.apache.ratis.proto.RaftProtos.RaftClientRequestProto.TypeCase.STALEREAD;
import static org.apache.ratis.proto.RaftProtos.RaftClientRequestProto.TypeCase.WATCH;
import static org.apache.ratis.proto.RaftProtos.RaftClientRequestProto.TypeCase.WRITE;

import java.util.Objects;

import org.apache.ratis.proto.RaftProtos.RaftClientRequestProto;
import org.apache.ratis.proto.RaftProtos.ReadRequestTypeProto;
import org.apache.ratis.proto.RaftProtos.ReplicationLevel;
import org.apache.ratis.proto.RaftProtos.SlidingWindowEntry;
import org.apache.ratis.proto.RaftProtos.StaleReadRequestTypeProto;
import org.apache.ratis.proto.RaftProtos.WatchRequestTypeProto;
import org.apache.ratis.proto.RaftProtos.WriteRequestTypeProto;
import org.apache.ratis.util.Preconditions;
import org.apache.ratis.util.ProtoUtils;

import io.opentracing.Span;

/**
 * Request from client to server
 */
public class RaftClientRequest extends RaftClientMessage {
  private static final Type WRITE_DEFAULT = new Type(WriteRequestTypeProto.getDefaultInstance());

  private static final Type DEFAULT_READ = new Type(ReadRequestTypeProto.getDefaultInstance());
  private static final Type DEFAULT_STALE_READ = new Type(StaleReadRequestTypeProto.getDefaultInstance());

  public static Type writeRequestType() {
    return WRITE_DEFAULT;
  }

  public static Type readRequestType() {
    return DEFAULT_READ;
  }

  public static Type staleReadRequestType(long minIndex) {
    return minIndex == 0L? DEFAULT_STALE_READ
        : new Type(StaleReadRequestTypeProto.newBuilder().setMinIndex(minIndex).build());
  }

  public static Type watchRequestType(long index, ReplicationLevel replication) {
    return new Type(WatchRequestTypeProto.newBuilder().setIndex(index).setReplication(replication).build());
  }

  /** The type of a request (oneof write, read, staleRead, watch; see the message RaftClientRequestProto). */
  public static class Type {
    public static Type valueOf(WriteRequestTypeProto write) {
      return WRITE_DEFAULT;
    }

    public static Type valueOf(ReadRequestTypeProto read) {
      return DEFAULT_READ;
    }

    public static Type valueOf(StaleReadRequestTypeProto staleRead) {
      return staleRead.getMinIndex() == 0? DEFAULT_STALE_READ
          : new Type(staleRead);
    }

    public static Type valueOf(WatchRequestTypeProto watch) {
      return watchRequestType(watch.getIndex(), watch.getReplication());
    }

    /**
     * The type case of the proto.
     * Only the corresponding proto (must be non-null) is used.
     * The other protos are ignored.
     */
    private final RaftClientRequestProto.TypeCase typeCase;
    private final Object proto;

    private Type(RaftClientRequestProto.TypeCase typeCase, Object proto) {
      this.typeCase = Objects.requireNonNull(typeCase, "typeCase == null");
      this.proto = Objects.requireNonNull(proto, "proto == null");
    }

    private Type(WriteRequestTypeProto write) {
      this(WRITE, write);
    }

    private Type(ReadRequestTypeProto read) {
      this(READ, read);
    }

    private Type(StaleReadRequestTypeProto staleRead) {
      this(STALEREAD, staleRead);
    }

    private Type(WatchRequestTypeProto watch) {
      this(WATCH, watch);
    }

    public boolean is(RaftClientRequestProto.TypeCase typeCase) {
      return getTypeCase().equals(typeCase);
    }

    public RaftClientRequestProto.TypeCase getTypeCase() {
      return typeCase;
    }

    public WriteRequestTypeProto getWrite() {
      Preconditions.assertTrue(is(WRITE));
      return (WriteRequestTypeProto)proto;
    }

    public ReadRequestTypeProto getRead() {
      Preconditions.assertTrue(is(READ));
      return (ReadRequestTypeProto)proto;
    }

    public StaleReadRequestTypeProto getStaleRead() {
      Preconditions.assertTrue(is(STALEREAD));
      return (StaleReadRequestTypeProto)proto;
    }

    public WatchRequestTypeProto getWatch() {
      Preconditions.assertTrue(is(WATCH));
      return (WatchRequestTypeProto)proto;
    }

    static String toString(ReplicationLevel replication) {
      return replication == ReplicationLevel.MAJORITY? "": "-" + replication;
    }

    public static String toString(WatchRequestTypeProto w) {
      return "Watch" + toString(w.getReplication()) + "(" + w.getIndex() + ")";
    }

    @Override
    public String toString() {
      switch (typeCase) {
        case WRITE:
          return "RW";
        case READ:
          return "RO";
        case STALEREAD:
          return "StaleRead(" + getStaleRead().getMinIndex() + ")";
        case WATCH:
          return toString(getWatch());
        default:
          throw new IllegalStateException("Unexpected request type: " + typeCase);
      }
    }
  }

  private final long callId;
  private final Message message;
  private final Type type;

  private final SlidingWindowEntry slidingWindowEntry;
  private final Span span;

  public RaftClientRequest(ClientId clientId, RaftPeerId serverId, RaftGroupId groupId, long callId, Type type) {
    this(clientId, serverId, groupId, callId, null, type, null, null);
  }

  public RaftClientRequest(
      ClientId clientId, RaftPeerId serverId, RaftGroupId groupId,
      long callId, Message message, Type type, SlidingWindowEntry slidingWindowEntry, Span span) {
    super(clientId, serverId, groupId);
    this.callId = callId;
    this.message = message;
    this.type = type;
    this.slidingWindowEntry = slidingWindowEntry != null? slidingWindowEntry: SlidingWindowEntry.getDefaultInstance();
    this.span = span;
  }

  @Override
  public final boolean isRequest() {
    return true;
  }

  public long getCallId() {
    return callId;
  }

  public SlidingWindowEntry getSlidingWindowEntry() {
    return slidingWindowEntry;
  }

  public Message getMessage() {
    return message;
  }

  public Type getType() {
    return type;
  }

  public Span getSpan() {
    return span;
  }

  public boolean is(RaftClientRequestProto.TypeCase typeCase) {
    return getType().is(typeCase);
  }

  @Override
  public String toString() {
    return super.toString() + ", cid=" + callId + ", seq=" + ProtoUtils.toString(slidingWindowEntry) + ", "
        + type + ", " + getMessage();
  }
}
