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

import java.util.Objects;

/**
 * Test context enables using different parameters for baseline/candidate.
 * It can be handy when we want to run the same task against the same Lucene code,
 * but test two different implementations of a feature.
 */
public class TestContext {
    public final FacetMode facetMode;

    private TestContext(FacetMode facetMode) {
        this.facetMode = Objects.requireNonNull(facetMode);
    }

    public static TestContext parse(String context) {
        FacetMode facetMode = FacetMode.UNDEFINED;
        if (context.isEmpty() == false) {
            String[] contextParams = context.split(",");
            for (String param : contextParams) {
                String[] keyValue = param.split(":");
                if (keyValue.length != 2) {
                    throw new IllegalArgumentException("Test context params must be key value pairs separated by colon, got: " + param);
                }
                if (keyValue[0].equals("facetMode")) {
                    facetMode = FacetMode.valueOf(keyValue[1]);
                }
            }
        }
        return new TestContext(facetMode);
    }

    public enum FacetMode {
        UNDEFINED,
        CLASSIC,
        SANDBOX,  // TODO: better names?
    }

}
