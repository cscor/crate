/*
 * Licensed to Crate under one or more contributor license agreements.
 * See the NOTICE file distributed with this work for additional
 * information regarding copyright ownership.  Crate licenses this file
 * to you under the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.  You may
 * obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied.  See the License for the specific language governing
 * permissions and limitations under the License.
 *
 * However, if you have executed another commercial license agreement
 * with Crate these terms will supersede the license and you may use the
 * software solely pursuant to the terms of the relevant commercial
 * agreement.
 */

package io.crate.metadata.settings;

import com.google.common.annotations.VisibleForTesting;
import io.crate.metadata.SearchPath;
import org.elasticsearch.Version;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;

import java.io.IOException;
import java.util.HashSet;
import java.util.Objects;
import java.util.Set;

public final class SessionSettings implements Writeable {

    private final String userName;
    private final SearchPath searchPath;
    private final boolean hashJoinsEnabled;
    private final Set<String> excludedOptimizerRules;

    public SessionSettings(StreamInput in) throws IOException {
        this.userName = in.readString();
        this.searchPath = SearchPath.createSearchPathFrom(in);
        this.hashJoinsEnabled = in.readBoolean();
        if (in.getVersion().onOrAfter(Version.V_4_2_0)) {
            int ruleSize = in.readVInt();
            excludedOptimizerRules = new HashSet<>(ruleSize);
            if (ruleSize > 0) {
                for (int i = 0; i < ruleSize; i++) {
                    excludedOptimizerRules.add(in.readString());
                }
            }
        } else {
            excludedOptimizerRules = Set.of();
        }
    }

    @VisibleForTesting
    public SessionSettings(String userName, SearchPath searchPath) {
        this(userName, searchPath, true, Set.of());
    }

    public SessionSettings(String userName, SearchPath searchPath, boolean hashJoinsEnabled, Set<String> rules) {
        this.userName = userName;
        this.searchPath = searchPath;
        this.hashJoinsEnabled = hashJoinsEnabled;
        this.excludedOptimizerRules = rules;
    }

    public String userName() {
        return userName;
    }

    public String currentSchema() {
        return searchPath.currentSchema();
    }

    public SearchPath searchPath() {
        return searchPath;
    }

    public boolean hashJoinsEnabled() {
        return hashJoinsEnabled;
    }

    public Set<String> excludedOptimizerRules() {
        return excludedOptimizerRules;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(userName);
        searchPath.writeTo(out);
        out.writeBoolean(hashJoinsEnabled);
        if (out.getVersion().onOrAfter(Version.V_4_2_0)) {
            out.writeVInt(excludedOptimizerRules.size());
            for (String rule : excludedOptimizerRules) {
                out.writeString(rule);
            }
        }
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        SessionSettings that = (SessionSettings) o;
        return Objects.equals(userName, that.userName) &&
               Objects.equals(searchPath, that.searchPath) &&
               Objects.equals(hashJoinsEnabled, that.hashJoinsEnabled) &&
               Objects.equals(excludedOptimizerRules, that.excludedOptimizerRules);
    }

    @Override
    public int hashCode() {
        return Objects.hash(userName, searchPath, hashJoinsEnabled, excludedOptimizerRules);
    }
}
