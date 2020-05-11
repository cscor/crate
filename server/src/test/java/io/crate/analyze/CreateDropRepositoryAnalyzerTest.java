/*
 * Licensed to CRATE Technology GmbH ("Crate") under one or more contributor
 * license agreements.  See the NOTICE file distributed with this work for
 * additional information regarding copyright ownership.  Crate licenses
 * this file to you under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.  You may
 * obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.  See the
 * License for the specific language governing permissions and limitations
 * under the License.
 *
 * However, if you have executed another commercial license agreement
 * with Crate these terms will supersede the license and you may use the
 * software solely pursuant to the terms of the relevant commercial agreement.
 */

package io.crate.analyze;

import static org.hamcrest.Matchers.allOf;
import static org.hamcrest.Matchers.hasEntry;
import static org.hamcrest.Matchers.is;

import java.util.Collections;
import java.util.Map;

import org.elasticsearch.action.admin.cluster.repositories.put.PutRepositoryRequest;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.MetaData;
import org.elasticsearch.cluster.metadata.RepositoriesMetaData;
import org.elasticsearch.cluster.metadata.RepositoryMetaData;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.test.ClusterServiceUtils;
import org.junit.Before;
import org.junit.Test;

import io.crate.analyze.repositories.RepositoryParamValidator;
import io.crate.data.Row;
import io.crate.exceptions.RepositoryAlreadyExistsException;
import io.crate.exceptions.RepositoryUnknownException;
import io.crate.planner.PlannerContext;
import io.crate.planner.node.ddl.CreateRepositoryPlan;
import io.crate.planner.operators.SubQueryResults;
import io.crate.test.integration.CrateDummyClusterServiceUnitTest;
import io.crate.testing.SQLExecutor;

public class CreateDropRepositoryAnalyzerTest extends CrateDummyClusterServiceUnitTest {

    private SQLExecutor e;
    private PlannerContext plannerContext;
    private RepositoryParamValidator repositoryParamValidator;

    @Before
    public void prepare() {
        RepositoriesMetaData repositoriesMetaData = new RepositoriesMetaData(
            Collections.singletonList(
                new RepositoryMetaData(
                    "my_repo",
                    "fs",
                    Settings.builder().put("location", "/tmp/my_repo").build()
                )));
        ClusterState clusterState = ClusterState.builder(new ClusterName("testing"))
            .metaData(MetaData.builder()
                          .putCustom(RepositoriesMetaData.TYPE, repositoriesMetaData))
            .build();
        ClusterServiceUtils.setState(clusterService, clusterState);
        e = SQLExecutor.builder(clusterService).build();
        plannerContext = e.getPlannerContext(clusterService.state());
        repositoryParamValidator = new RepositoryParamValidator(Map.of());
    }

    @SuppressWarnings("unchecked")
    private <S> S analyze(SQLExecutor e, String stmt) {
        AnalyzedStatement analyzedStatement = e.analyze(stmt);
        if (analyzedStatement instanceof AnalyzedCreateRepository) {
            return (S) CreateRepositoryPlan.createRequest(
                (AnalyzedCreateRepository) analyzedStatement,
                plannerContext.transactionContext(),
                plannerContext.functions(),
                Row.EMPTY,
                SubQueryResults.EMPTY,
                repositoryParamValidator);
        } else if (analyzedStatement instanceof AnalyzedDropRepository) {
            return (S) analyzedStatement;
        } else {
            throw new AssertionError("Statement of type " + analyzedStatement.getClass() + " not supported");
        }
    }

    @Test
    public void testCreateRepository() {
        PutRepositoryRequest request = analyze(
            e,
            "CREATE REPOSITORY \"new_repository\" TYPE \"fs\" WITH (" +
            "   location='/mount/backups/my_backup'," +
            "   compress=True)");
        assertThat(request.name(), is("new_repository"));
        assertThat(request.type(), is("fs"));
        assertThat(
            request.settings().getAsStructuredMap(),
            allOf(
                hasEntry("compress", "true"),
                hasEntry("location", "/mount/backups/my_backup"))
        );
    }

    @Test
    public void testCreateExistingRepository() {
        expectedException.expect(RepositoryAlreadyExistsException.class);
        expectedException.expectMessage("Repository 'my_repo' already exists");
        analyze(e, "CREATE REPOSITORY my_repo TYPE fs");
    }

    @Test
    public void testDropUnknownRepository() {
        expectedException.expect(RepositoryUnknownException.class);
        expectedException.expectMessage("Repository 'unknown_repo' unknown");
        analyze(e, "DROP REPOSITORY \"unknown_repo\"");
    }

    @Test
    public void testDropExistingRepo() {
        AnalyzedDropRepository statement = analyze(e, "DROP REPOSITORY my_repo");
        assertThat(statement.name(), is("my_repo"));
    }

    @Test
    public void testCreateAzureRepositoryWithAllSettings() {
        PutRepositoryRequest request = analyze(
            e,
            "CREATE REPOSITORY foo TYPE azure WITH (" +
            "   container='test_container'," +
            "   base_path='test_path'," +
            "   chunk_size='12mb'," +
            "   compress=true," +
            "   readonly=true," +
            "   location_mode='primary_only'," +
            "   account='test_account'," +
            "   key='test_key'," +
            "   max_retries=3," +
            "   endpoint_suffix='.com'," +
            "   timeout='30s'," +
            "   proxy_port=0," +
            "   proxy_type='socks'," +
            "   proxy_host='localhost')");
        assertThat(request.name(), is("foo"));
        assertThat(request.type(), is("azure"));
        assertThat(
            request.settings().getAsStructuredMap(),
            allOf(
                hasEntry("container", "test_container"),
                hasEntry("base_path", "test_path"),
                hasEntry("chunk_size", "12mb"),
                hasEntry("compress", "true"),
                hasEntry("readonly", "true"),
                hasEntry("account", "test_account"),
                hasEntry("key", "test_key"),
                hasEntry("max_retries", "3"),
                hasEntry("endpoint_suffix", ".com"),
                hasEntry("timeout", "30s"),
                hasEntry("proxy_port", "0"),
                hasEntry("proxy_type", "socks"),
                hasEntry("proxy_host", "localhost")
            )
        );
    }

    @Test
    public void testCreateAzureRepoWithMissingMandatorySettings() {
        expectedException.expect(IllegalArgumentException.class);
        expectedException.expectMessage("The following required parameters are missing to" +
                                        " create a repository of type \"azure\": [key]");
        analyze(e, "CREATE REPOSITORY foo TYPE azure WITH (account='test')");
    }

    @Test
    public void testCreateAzureRepoWithWrongSettings() {
        expectedException.expect(IllegalArgumentException.class);
        expectedException.expectMessage("setting 'wrong' not supported");
        analyze(e, "CREATE REPOSITORY foo TYPE azure WITH (wrong=true)");
    }
}
