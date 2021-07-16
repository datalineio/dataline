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

package io.airbyte.db;

import java.sql.Date;
import java.sql.SQLException;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.time.Instant;
import java.util.function.Function;

public class DataTypeUtils {

  public static final DateFormat DATE_FORMAT = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss'Z'"); // Quoted "Z" to indicate UTC, no timezone offset

  public static <T> T returnNullIfInvalid(DataTypeSupplier<T> valueProducer) {
    return returnNullIfInvalid(valueProducer, ignored -> true);
  }

  public static <T> T returnNullIfInvalid(DataTypeSupplier<T> valueProducer, Function<T, Boolean> isValidFn) {
    // Some edge case values (e.g: Infinity, NaN) have no java or JSON equivalent, and will throw an
    // exception when parsed. We want to parse those
    // values as null.
    // This method reduces error handling boilerplate.
    try {
      T value = valueProducer.apply();
      return isValidFn.apply(value) ? value : null;
    } catch (SQLException e) {
      return null;
    }
  }

  public static String toISO8601String(long epochMillis) {
    return DATE_FORMAT.format(Date.from(Instant.ofEpochMilli(epochMillis)));
  }

  public static String toISO8601String(java.util.Date date) {
    return DATE_FORMAT.format(date);
  }

}
