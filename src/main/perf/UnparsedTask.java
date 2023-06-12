/*
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
package perf;

import org.apache.lucene.queryparser.classic.ParseException;

import java.io.IOException;
import java.io.PrintStream;
import java.util.concurrent.atomic.AtomicReference;

/**
 * The task that contains only category and unparsed text
 * and will only be parsed when running
 */
public class UnparsedTask extends Task{
    private final String category;
    private final String origText;
    private final AtomicReference<Task> parsedTask = new AtomicReference<>();

    public UnparsedTask(String[] categoryAndText) {
        this.category = categoryAndText[0];
        this.origText = categoryAndText[1];
    }

    private UnparsedTask(String category, String origText) {
        this.category = category;
        this.origText = origText;
    }


    @Override
    public void go(IndexState state, TaskParser taskParser) throws IOException {
        if (parsedTask.get() != null) {
            throw new IllegalStateException("Task cannot be reused");
        }
        try {
            parsedTask.set(taskParser.secondPassParse(this));
        } catch (ParseException parseException) {
            throw new RuntimeException(parseException);
        }
        parsedTask.get().go(state, taskParser);
        this.totalHitCount = parsedTask.get().totalHitCount;
        this.runTimeNanos = parsedTask.get().runTimeNanos;
    }

    @Override
    public String getCategory() {
        return category;
    }

    public String getOrigText() {
        return origText;
    }

    @Override
    public Task clone() {
        return new UnparsedTask(category, origText);
    }

    @Override
    public long checksum() {
        assert parsedTask.get() != null: "task should be already parsed before this method";
        return parsedTask.get().checksum();
    }

    @Override
    public void printResults(PrintStream out, IndexState state) throws IOException {
        assert parsedTask.get() != null: "task should be already parsed before this method";
        parsedTask.get().printResults(out, state);
    }

    @Override
    public String toString() {
        if (parsedTask.get() != null) {
            return parsedTask.toString();
        } else {
            return "UnparsedTask cat:" + category + " text:" + origText;
        }
    }
}
