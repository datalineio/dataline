/*
 * MIT License
 *
 * Copyright (c) 2020 Airbyte
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in all
 * copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
 * SOFTWARE.
 */

package io.airbyte.server.errors;

import io.airbyte.api.model.KnownExceptionInfo;
import org.apache.logging.log4j.core.util.Throwables;

public abstract class KnownException extends RuntimeException {

  public KnownException(String message) {
    super(message);
  }

  public KnownException(String message, Throwable cause) {
    super(message, cause);
  }

  abstract public int getHttpCode();

  public KnownExceptionInfo asKnownExceptionInfo() {
    return KnownException.infoFromThrowable(this);
  }

  public static KnownExceptionInfo infoFromThrowableWithMessage(Throwable t, String message) {
    KnownExceptionInfo exceptionInfo = new KnownExceptionInfo()
        .exceptionClassName(t.getClass().getName())
        .message(message)
        .exceptionStack(Throwables.toStringList(t));
    if (t.getCause() != null) {
      exceptionInfo.rootCauseExceptionClassName(t.getClass().getClass().getName());
      exceptionInfo.rootCauseExceptionStack(Throwables.toStringList(t.getCause()));
    }
    return exceptionInfo;
  }

  public static KnownExceptionInfo infoFromThrowable(Throwable t) {
    return infoFromThrowableWithMessage(t, t.getMessage());
  }

}
