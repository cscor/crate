/*
 * This file is part of a module with proprietary Enterprise Features.
 *
 * Licensed to Crate.io Inc. ("Crate.io") under one or more contributor
 * license agreements.  See the NOTICE file distributed with this work for
 * additional information regarding copyright ownership.
 *
 * Unauthorized copying of this file, via any medium is strictly prohibited.
 *
 * To use this file, Crate.io must have given you permission to enable and
 * use such Enterprise Features and you must have a valid Enterprise or
 * Subscription Agreement with Crate.io.  If you enable or use the Enterprise
 * Features, you represent and warrant that you have a valid Enterprise or
 * Subscription Agreement with Crate.io.  Your use of the Enterprise Features
 * if governed by the terms and conditions of your Enterprise or Subscription
 * Agreement with Crate.io.
 */

package io.crate.auth.user;

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import io.crate.action.sql.SessionContext;
import io.crate.analyze.user.Privilege;
import io.crate.exceptions.RelationValidationException;
import io.crate.exceptions.SchemaUnknownException;
import io.crate.exceptions.UnhandledServerException;
import io.crate.exceptions.UnsupportedFeatureException;
import io.crate.metadata.RelationName;
import io.crate.test.integration.CrateUnitTest;
import org.hamcrest.Matcher;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.hasItem;
import static org.hamcrest.core.Is.is;

public class AccessControlMaySeeTest extends CrateUnitTest {

    private List<List<Object>> validationCallArguments;
    private User user;
    private AccessControl accessControl;

    @Before
    public void setUpUserAndValidator() {
        validationCallArguments = new ArrayList<>();
        user = new User("normal", ImmutableSet.of(), ImmutableSet.of(), null) {

            @Override
            public boolean hasAnyPrivilege(Privilege.Clazz clazz, String ident) {
                validationCallArguments.add(Lists.newArrayList(clazz, ident, user.name()));
                return true;
            }
        };
        accessControl = new AccessControlImpl(userName -> user, new SessionContext(Set.of(), user, Set.of()));
    }

    @SuppressWarnings("unchecked")
    private void assertAskedAnyForCluster() {
        Matcher<Iterable<?>> matcher = (Matcher) hasItem(contains(Privilege.Clazz.CLUSTER, null, user.name()));
        assertThat(validationCallArguments, matcher);
    }

    @SuppressWarnings("unchecked")
    private void assertAskedAnyForSchema(String ident) {
        Matcher<Iterable<?>> matcher = (Matcher) hasItem(contains(Privilege.Clazz.SCHEMA, ident, user.name()));
        assertThat(validationCallArguments, matcher);
    }

    @SuppressWarnings("unchecked")
    private void assertAskedAnyForTable(String ident) {
        Matcher<Iterable<?>> matcher = (Matcher) hasItem(contains(Privilege.Clazz.TABLE, ident, user.name()));
        assertThat(validationCallArguments, matcher);
    }

    @Test
    public void testTableScopeException() throws Exception {
        accessControl.ensureMaySee(new RelationValidationException(Lists.newArrayList(
            RelationName.fromIndexName("users"),
            RelationName.fromIndexName("my_schema.foo")
        ), "bla"));
        assertAskedAnyForTable("doc.users");
        assertAskedAnyForTable("my_schema.foo");
    }

    @Test
    public void testSchemaScopeException() throws Exception {
        accessControl.ensureMaySee(new SchemaUnknownException("my_schema"));
        assertAskedAnyForSchema("my_schema");
    }

    @Test
    public void testClusterScopeException() throws Exception {
        accessControl.ensureMaySee(new UnsupportedFeatureException("unsupported"));
        assertAskedAnyForCluster();
    }

    @Test
    public void testUnscopedException() throws Exception {
        accessControl.ensureMaySee(new UnhandledServerException("unhandled"));
        assertThat(validationCallArguments.size(), is(0));
    }
}
