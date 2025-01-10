package com.daiichi;

import com.daiichi.pojo.ExecutionPlan;
import com.daiichi.pojo.Node;
import jakarta.annotation.PreDestroy;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.tinkerpop.gremlin.driver.Client;
import org.apache.tinkerpop.gremlin.driver.Result;
import org.springframework.stereotype.Service;

import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

import static com.daiichi.pojo.Constants.*;

@Slf4j
@Service
@RequiredArgsConstructor
public class NeptuneBWrapper implements DBWrapper {
    private final Client client;

    @Override
    public void createTree(ExecutionPlan plan) {
        String nodeData = plan.getGraph().stream()
            .filter(node -> node.getParentId() == null)
            .findFirst()
            .map(Node::getData)
            .orElse(null);

        String query = "g.V().hasLabel('%s').has('data', '%s')".formatted(EXECUTION_PLAN_NODE_CLASS, nodeData);
        Iterator<?> resultSet = client.submit(query).stream().iterator();
        if (resultSet.hasNext()) {
            String resetQuery = "g.V().hasLabel('%s').has('planId', '%s').has('status', neq('Pending')).property('status', 'Pending')".formatted(EXECUTION_PLAN_NODE_CLASS, plan.getId());
            long count = client.submit(resetQuery).stream().count();
            log.info("Plan {} already exists, reset status of {} nodes", plan.getId(), count);
            return;
        }

        Map<String, Node> vertexMap = plan.getGraph().stream()
            .peek(node -> client.submit(mapToVertex(node)))
            .collect(Collectors.toMap(Node::getId, Function.identity()));
        //noinspection MappingBeforeCount
        long edgeCount = plan.getGraph().stream()
            .filter(node -> node.getParentId() != null)
            .map(node -> client.submit("""
                g.V().hasLabel('%s').has('id', '%s').as('source')
                 .V().hasLabel('%s').has('id', '%s').as('target')
                 .addE('%s').from('source').to('target')
                """
                .formatted(EXECUTION_PLAN_NODE_CLASS, node.getParentId(), EXECUTION_PLAN_NODE_CLASS, node.getId(), EXECUTION_PLAN_DEPENDS_CLASS)))
            .count();

        log.info("Stored graph {} in Neptune, with {} vertices and {} edges", plan.getId(), vertexMap.size(), edgeCount);
    }

    @Override
    public void fetchAndUpdate(String planId) {
        try {
            String query = """
                g.V().hasLabel('%s').has('data', '%s')
                .repeat(outE().inV())
                .until(has('status', neq('Completed')))
                .limit(1)
                .valueMap(true)
                """.formatted(EXECUTION_PLAN_NODE_CLASS, EXECUTION_PLAN_ROOT_NODE.formatted(planId));

            Iterator<Result> resultSet = client.submit(query).stream().iterator();
            if (resultSet.hasNext()) {
                Result nodeToUpdate = resultSet.next();
                Node node = mapToNode(nodeToUpdate);
                node.progressStatus();
                String updateQuery = "g.V().hasLabel('%s').has('id', '%s').property('status', '%s')".formatted(EXECUTION_PLAN_NODE_CLASS, node.getId(), node.getStatus());
                client.submit(updateQuery);
                log.info("Updated node {} in OrientDB to status {}", node.getData(), node.getStatus());
            }
        } catch (Exception e) {
            log.error("Error during OrientDB fetch and update", e);
        }

    }

    @PreDestroy
    public void closeSession() {
        if (!client.isClosing()) {
            client.getCluster().close();
            System.out.println("OrientDB session closed successfully.");
        }
    }

    private String mapToVertex(Node node) {
        // Create vertices
        return """
                g.addV('%s')
                .property('id', '%s')
                .property('planId', '%s')
                .property('data', '%s')
                .property('status', '%s')
                .property('parentId', '%s')
            """.formatted(EXECUTION_PLAN_NODE_CLASS, node.getId(), node.getPlanId(), node.getData(), node.getStatus(), node.getParentId());
    }

    private Node mapToNode(Result nodeToUpdate) {
        //noinspection unchecked
        HashMap<String, List<String>> nodeMap = nodeToUpdate.get(HashMap.class);
        return Node.builder()
            .id(nodeMap.get("id").get(0))
            .planId(nodeMap.get("planId").get(0))
            .data(nodeMap.get("data").get(0))
            .status(Node.Status.valueOf(nodeMap.get("status").get(0)))
            .parentId(nodeMap.get("parentId").get(0).equals("null") ? null : nodeMap.get("parentId").get(0))
            .build();
    }
}
