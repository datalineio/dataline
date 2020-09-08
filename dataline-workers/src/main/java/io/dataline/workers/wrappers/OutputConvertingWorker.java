/*
 * MIT License
 *
 * Copyright (c) 2020 Dataline
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

package io.dataline.workers.wrappers;

import io.dataline.workers.InvalidCatalogException;
import io.dataline.workers.InvalidCredentialsException;
import io.dataline.workers.OutputAndStatus;
import io.dataline.workers.Worker;
import java.nio.file.Path;
import java.util.function.Function;

abstract class OutputConvertingWorker<InputType, OriginalOutputType, FinalOutputType> implements Worker<InputType, FinalOutputType> {

  private final Worker<InputType, OriginalOutputType> innerWorker;
  private Function<OriginalOutputType, FinalOutputType> convertFn;

  public OutputConvertingWorker(Worker<InputType, OriginalOutputType> innerWorker, Function<OriginalOutputType, FinalOutputType> convertFn) {
    this.innerWorker = innerWorker;
    this.convertFn = convertFn;
  }

  @Override
  public OutputAndStatus<FinalOutputType> run(InputType config, Path jobRoot) throws InvalidCredentialsException, InvalidCatalogException {
    OutputAndStatus<OriginalOutputType> run = innerWorker.run(config, jobRoot);
    if (run.getOutput().isPresent()) {
      return new OutputAndStatus<FinalOutputType>(run.getStatus(), convertFn.apply(run.getOutput().get()));
    } else {
      return new OutputAndStatus<>(run.getStatus());
    }
  }

  @Override
  public void cancel() {
    innerWorker.cancel();
  }

}
