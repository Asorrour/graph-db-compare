package com.daiichi;

import com.arangodb.ArangoDB;
import com.arangodb.ArangoDatabase;
import com.arangodb.entity.CollectionType;
import com.arangodb.entity.EdgeDefinition;
import com.arangodb.model.CollectionCreateOptions;
import com.orientechnologies.orient.core.db.ODatabaseSession;
import com.orientechnologies.orient.core.db.ODatabaseType;
import com.orientechnologies.orient.core.db.OrientDB;
import com.orientechnologies.orient.core.db.OrientDBConfig;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.tinkerpop.gremlin.driver.Client;
import org.apache.tinkerpop.gremlin.driver.Cluster;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.util.Collections;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static com.daiichi.pojo.Constants.*;

@Slf4j
@Configuration
@RequiredArgsConstructor
class DbConfig {

    private final String DB_NAME = "benchmarkdb";

    @Bean
    public ArangoDB arangoDB() {
        return new ArangoDB.Builder().host("localhost", 8529).user("root").password("password").build();
    }

    @Bean
    public OrientDB orientDB() {
        return new OrientDB("remote:localhost", "root", "admin", OrientDBConfig.defaultConfig());
    }

    @Bean
    public Client neptuneDB() {
        Cluster cluster = Cluster.build()
            .addContactPoint("localhost")
            .port(8182)
            .create();
        return cluster.connect();
    }

    @Bean
    public ArangoDatabase arangoDatabase(ArangoDB arangoDB) {
        if (!arangoDB.db(DB_NAME).exists()) {
            arangoDB.createDatabase(DB_NAME);
        }
        ArangoDatabase arangoDatabase = arangoDB.db(DB_NAME);
        if (!arangoDatabase.collection(EXECUTION_PLAN_NODE_CLASS).exists()) {
            arangoDatabase.createCollection(EXECUTION_PLAN_NODE_CLASS);
        }
        if (!arangoDatabase.collection(EXECUTION_PLAN_DEPENDS_CLASS).exists()) {
            arangoDatabase.createCollection(EXECUTION_PLAN_DEPENDS_CLASS, new CollectionCreateOptions().type(CollectionType.EDGES));
        }
        EdgeDefinition edgeDefinition = new EdgeDefinition()
            .collection(EXECUTION_PLAN_DEPENDS_CLASS)
            .from(EXECUTION_PLAN_NODE_CLASS)
            .to(EXECUTION_PLAN_NODE_CLASS);
        if (!arangoDatabase.graph(EXECUTION_PLAN_GRAPH_CLASS).exists()) {
            arangoDatabase.createGraph(EXECUTION_PLAN_GRAPH_CLASS, Collections.singletonList(edgeDefinition));
        }
        return arangoDatabase;
    }

    @Bean
    public ODatabaseSession orientSession(OrientDB orientDB) {
        if (!orientDB.exists(DB_NAME)) {
            orientDB.create(DB_NAME, ODatabaseType.PLOCAL);
        }
        ODatabaseSession orientSession = orientDB.open(DB_NAME, "root", "admin");
        if (orientSession.getClass(EXECUTION_PLAN_NODE_CLASS) == null) {
            orientSession.createVertexClass(EXECUTION_PLAN_NODE_CLASS);
        }
        if (orientSession.getClass(EXECUTION_PLAN_DEPENDS_CLASS) == null) {
            orientSession.createEdgeClass(EXECUTION_PLAN_DEPENDS_CLASS);
        }
        return orientSession;
    }

    @Bean
    public ExecutorService executorService() {
        return Executors.newFixedThreadPool(10); // Configurable pool size
    }
}
