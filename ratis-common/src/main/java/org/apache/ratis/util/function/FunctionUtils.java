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
package org.apache.ratis.util.function;

import java.util.function.Consumer;
import java.util.function.Function;

public interface FunctionUtils {
  /**
   * Convert the given consumer to a function with any output type
   * such that the returned function always returns null.
   */
  static <INPUT, OUTPUT> Function<INPUT, OUTPUT> consumerAsNullFunction(Consumer<INPUT> consumer) {
    return input -> {
      consumer.accept(input);
      return null;
    };
  }

  /**
   * Convert the given consumer to a function with output type matching its input type
   * such that the returned function always returns its input.
   */
  static <T> Function<T, T> consumerAsIdentity(Consumer<? super T> consumer) {
    return input -> {
      consumer.accept(input);
      return input;
    };
  }

}
