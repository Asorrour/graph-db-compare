package com.daiichi;

import com.daiichi.pojo.ExecutionPlan;
import com.daiichi.pojo.Node;
import com.orientechnologies.orient.core.db.ODatabaseSession;
import com.orientechnologies.orient.core.record.OElement;
import com.orientechnologies.orient.core.record.OVertex;
import com.orientechnologies.orient.core.sql.executor.OResultSet;
import jakarta.annotation.PreDestroy;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

import static com.daiichi.pojo.Constants.*;

@Slf4j
@Service
@RequiredArgsConstructor
public class OrientDBWrapper implements DBWrapper {

    private final ODatabaseSession orientSession;

    @Override
    public void createTree(ExecutionPlan plan) {
        orientSession.activateOnCurrentThread();
        String nodeData = plan.getGraph().stream()
            .filter(node -> node.getParentId() == null)
            .findFirst()
            .map(Node::getData)
            .orElse(null);
        String query = "SELECT * FROM %s WHERE data = '%s' LIMIT 1".formatted(EXECUTION_PLAN_NODE_CLASS, nodeData);

        OResultSet resultSet = orientSession.query(query);
        if (resultSet.hasNext()) {
            String resetQuery = "UPDATE %s SET status = 'Pending' WHERE NOT status = 'Pending' and planId = '%s'".formatted(EXECUTION_PLAN_NODE_CLASS, plan.getId());
            OResultSet updateResultSet = orientSession.command(resetQuery);
            if (updateResultSet.hasNext()) {
                log.info("Plan {} already exists, reset status of {} nodes", plan.getId(), updateResultSet.next().getProperty("count"));
            }
            return;
        }

        Map<String, OVertex> vertexMap = plan.getGraph().stream()
            .map(node -> mapToOVertex(node).<OVertex>save())
            .collect(Collectors.toMap(oVertex -> oVertex.getProperty("id"), Function.identity()));
        long edgeCount = plan.getGraph().stream()
            .filter(node -> node.getParentId() != null)
            .map(node -> vertexMap.get(node.getId())
                .addEdge(vertexMap.get(node.getParentId()), EXECUTION_PLAN_DEPENDS_CLASS)
                .save()).count();

        log.info("Stored graph {} in OrientDB, with {} vertices and {} edges", plan.getId(), vertexMap.size(), edgeCount);
    }

    @Override
    public void fetchAndUpdate(String planId) {
        orientSession.activateOnCurrentThread();
        try {
            // Recursive query to traverse and find the first eligible node
            String query = """
                SELECT * FROM (
                  TRAVERSE IN(DEPENDS_ON)
                  FROM (SELECT * FROM PLAN_NODE WHERE data LIKE '%s')
                  STRATEGY BREADTH_FIRST
                )
                WHERE NOT status = 'Completed'
                LIMIT 1
                """.formatted(EXECUTION_PLAN_ROOT_NODE.formatted(planId));

            OResultSet resultSet = orientSession.query(query);
            if (resultSet.hasNext()) {
                OElement nodeToUpdate = resultSet.next().toElement();

                // Update node status in Java and save back to the database
                Node node = mapToNode(nodeToUpdate); // Utility method to map ODocument to Node
                node.progressStatus();
                nodeToUpdate.setProperty("status", node.getStatus().toString());
                nodeToUpdate.save();
                log.info("Updated node {} in OrientDB to status {}", node.getData(), node.getStatus());
            }
        } catch (Exception e) {
            log.error("Error during OrientDB fetch and update", e);
        }
    }

    @PreDestroy
    public void closeSession() {
        orientSession.activateOnCurrentThread();
        if (!orientSession.isClosed()) {
            orientSession.close();
            System.out.println("OrientDB session closed successfully.");
        }
    }

    private OVertex mapToOVertex(Node node) {
        OVertex vertex = orientSession.newVertex(EXECUTION_PLAN_NODE_CLASS);
        vertex.setProperty("id", node.getId());
        vertex.setProperty("data", node.getData());
        vertex.setProperty("planId", node.getPlanId());
        vertex.setProperty("status", node.getStatus());
        vertex.setProperty("parentId", node.getParentId());
        return vertex;
    }

    private Node mapToNode(OElement document) {
        return Node.builder()
            .id(document.getProperty("id"))
            .data(document.getProperty("data"))
            .planId(document.getProperty("planId"))
            .status(Node.Status.valueOf(document.getProperty("status")))
            .parentId(document.getProperty("parentId"))
            .build();
    }
}
