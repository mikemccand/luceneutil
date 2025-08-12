package perf;

/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import java.io.IOException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.lucene.util.SameThreadExecutorService;

public class TaskThreads {  

  private final TaskThread[] threads;
  final CountDownLatch startLatch = new CountDownLatch(1);
  final CountDownLatch stopLatch;
  final AtomicBoolean stop;
  final AtomicReference<SearchPerfTest.ThreadDetails> endThreadDetails;
  private long startNanos;

  public TaskThreads(TaskSource tasks, IndexState indexState, int numConcurrentQueries, TaskParserFactory taskParserFactory, AtomicReference<SearchPerfTest.ThreadDetails> endThreadDetails) throws IOException {
    threads = new TaskThread[numConcurrentQueries];
    stopLatch = new CountDownLatch(numConcurrentQueries);
    stop = new AtomicBoolean(false);
    this.endThreadDetails = endThreadDetails;
    for(int threadIDX=0;threadIDX<numConcurrentQueries;threadIDX++) {
      threads[threadIDX] = new TaskThread(startLatch, stopLatch, stop, tasks, indexState, threadIDX, taskParserFactory.getTaskParser(), endThreadDetails);
      threads[threadIDX].start();
    }
  }

  public void start() {
    startLatch.countDown();
  }

  public void finish() throws InterruptedException {
    stopLatch.await();
  }

  public void stop() throws InterruptedException {
    stop.getAndSet(true);
    for (Thread t : threads) {
      t.join();
    }
  }

  private static class TaskThread extends Thread {
    private final CountDownLatch startLatch;
    private final CountDownLatch stopLatch;
    private final AtomicBoolean stop;
    private final TaskSource tasks;
    private final IndexState indexState;
    private final int threadID;
    private final TaskParser taskParser;
    private long tasksStopNanos = -1;
    private final AtomicReference<SearchPerfTest.ThreadDetails> endThreadDetails;

    public TaskThread(CountDownLatch startLatch, CountDownLatch stopLatch, AtomicBoolean stop, TaskSource tasks,
                      IndexState indexState, int threadID, TaskParser taskParser, AtomicReference<SearchPerfTest.ThreadDetails> endThreadDetails) {
      this.startLatch = startLatch;
      this.stopLatch = stopLatch;
      this.stop = stop;
      this.tasks = tasks;
      this.indexState = indexState;
      this.threadID = threadID;
      this.taskParser = taskParser;
      this.endThreadDetails = endThreadDetails;
    }

    public long getTasksStopNanos() {
      return tasksStopNanos;
    }

    @Override
    public void run() {
      try {
        startLatch.await();
      } catch (InterruptedException ie) {
        Thread.currentThread().interrupt();
        return;
      }

      ExecutorService executor = indexState.executor;
      if (executor == null) {
        executor = new SameThreadExecutorService();
      }

      try {
        while (stop.get() == false) {
          final Task task = tasks.nextTask();
          if (task == null) {
            // Done
            this.tasksStopNanos = System.nanoTime();
            // first thread that finishes snapshots all threads.  this way we do not include "winddown" time in our measurement.
            endThreadDetails.compareAndSet(null, new SearchPerfTest.ThreadDetails());
            break;
          }

          // Run the task in the IndexSearcher's executor. This is important because IndexSearcher#search also uses the current thread to
          // search, so not running #search from the executor would artificially use one more thread than configured via luceneutil.
          // We're counting time within the task to not include forking time for the top-level search in the reported time.
          final Task clonedTask = task.clone();
          executor.submit(() -> {
            clonedTask.startTimeNanos = System.nanoTime();
            try {
              clonedTask.go(indexState, taskParser);
            } catch (IOException ioe) {
              throw new RuntimeException(ioe);
            }
            try {
              tasks.taskDone(clonedTask, clonedTask.startTimeNanos-clonedTask.recvTimeNS, clonedTask.totalHitCount);
            } catch (Exception e) {
              System.out.println(Thread.currentThread().getName() + ": ignoring exc:");
              e.printStackTrace();
            }
            clonedTask.runTimeNanos = System.nanoTime()-clonedTask.startTimeNanos;
          }).get();
          
          // Copy results back to original task for reporting
          task.totalHitCount = clonedTask.totalHitCount;
          task.runTimeNanos = clonedTask.runTimeNanos;

          task.threadID = threadID;
        }
      } catch (Exception e) {
        throw new RuntimeException(e);
      } finally {
        stopLatch.countDown();
      }
    }
  }
}
