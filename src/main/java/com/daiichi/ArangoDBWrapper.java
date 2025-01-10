package com.daiichi;

import com.arangodb.ArangoCursor;
import com.arangodb.ArangoDB;
import com.arangodb.ArangoDatabase;
import com.arangodb.ArangoIterator;
import com.arangodb.entity.BaseDocument;
import com.daiichi.pojo.ExecutionPlan;
import com.daiichi.pojo.Node;
import jakarta.annotation.PreDestroy;
import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

import static com.daiichi.pojo.Constants.*;

@Slf4j
@Service
@AllArgsConstructor
public class ArangoDBWrapper implements DBWrapper {

    private final ArangoDB arangoDB;
    private final ArangoDatabase arangoDatabase;

    @Override
    public void createTree(ExecutionPlan plan) {
        String nodeData = plan.getGraph().stream()
            .filter(node -> node.getParentId() == null)
            .findFirst()
            .map(Node::getData)
            .orElse(null);
        String query = "FOR node IN %s FILTER node.data == '%s' LIMIT 1 RETURN node".formatted(EXECUTION_PLAN_NODE_CLASS, nodeData);
        ArangoIterator<BaseDocument> rootNode = arangoDatabase.query(query, BaseDocument.class).iterator();
        if (rootNode.hasNext()) {
            String resetQuery = "FOR node IN %s FILTER node.planId == @planId AND NOT node.status == 'Pending' UPDATE node WITH { status: 'Pending' } IN %s".formatted(EXECUTION_PLAN_NODE_CLASS, EXECUTION_PLAN_NODE_CLASS);
            Map<String, Object> bindVars = Map.of("planId", plan.getId());
            ArangoCursor<BaseDocument> cursor = arangoDatabase
                .query(resetQuery, BaseDocument.class, bindVars, null);
            if (cursor != null) {
                log.info("Plan {} already exists, reset status of {} nodes", plan.getId(), cursor.getStats().getWritesExecuted());
            }
            return;
        }

        Map<String, BaseDocument> vertexMap = plan.getGraph().stream()
            .map(node -> {
                BaseDocument document = mapToDocument(node);
                arangoDatabase.collection(EXECUTION_PLAN_NODE_CLASS).insertDocument(document);
                return document;
            })
            .collect(Collectors.toMap(BaseDocument::getId, Function.identity()));

        long edgeCount = plan.getGraph().stream()
            .filter(node -> node.getParentId() != null)
            .map(node -> {
                BaseDocument edge = new BaseDocument();
                edge.addAttribute("_from", EXECUTION_PLAN_NODE_CLASS + "/" + node.getParentId());
                edge.addAttribute("_to", EXECUTION_PLAN_NODE_CLASS + "/" + node.getId());
                arangoDatabase.collection(EXECUTION_PLAN_DEPENDS_CLASS).insertDocument(edge);
                return edge;
            }).count();

        log.info("Stored graph {} in OrientDB, with {} vertices and {} edges", plan.getId(), vertexMap.size(), edgeCount);
    }

    @Override
    public void fetchAndUpdate(String planId) {
        try {
            // Query to traverse the graph starting from the root node and find the first eligible node
            String query = """
                FOR node IN %s FILTER node.data == @rootData LIMIT 1
                  FOR v, e IN 1..10 OUTBOUND
                    node._id %s
                    OPTIONS {bfs: true}
                    FILTER v.planId == @planId
                    FILTER v.status != 'Completed'
                    LIMIT 1
                    RETURN v""".formatted(EXECUTION_PLAN_NODE_CLASS, EXECUTION_PLAN_DEPENDS_CLASS);

            Map<String, Object> bindVars = Map.of("rootData", EXECUTION_PLAN_ROOT_NODE.formatted(planId), "planId", planId);

            ArangoIterator<BaseDocument> nodeToUpdateItr = arangoDatabase.query(query, BaseDocument.class, bindVars, null).iterator();
            if (nodeToUpdateItr.hasNext()) {
                // Update node status in Java and save back to the database
                BaseDocument nodeToUpdate = nodeToUpdateItr.next();
                String nodeId = nodeToUpdate.getKey();
                Node node = mapToNode(nodeToUpdate); // Utility method to map BaseDocument to Node
                node.progressStatus();

                nodeToUpdate.addAttribute("status", node.getStatus().toString());
                arangoDatabase.collection(EXECUTION_PLAN_NODE_CLASS).updateDocument(nodeId, nodeToUpdate);

                log.info("Updated node {} in ArangoDB to status {}", nodeId, node.getStatus());
            }
        } catch (Exception e) {
            log.error("Error during ArangoDB fetch and update", e);
        }
    }

    @PreDestroy
    public void closeDatabase() {
        if (arangoDB != null) {
            arangoDB.shutdown();
            System.out.println("ArangoDB session closed successfully.");
        }
    }

    private BaseDocument mapToDocument(Node node) {
        BaseDocument document = new BaseDocument(node.getId());
        document.setId(node.getId());
        document.addAttribute("data", node.getData());
        document.addAttribute("planId", node.getPlanId());
        document.addAttribute("status", node.getStatus());
        document.addAttribute("parentId", node.getParentId());
        return document;
    }

    private Node mapToNode(BaseDocument document) {
        return Node.builder()
            .id(document.getKey())
            .data((String) document.getAttribute("data"))
            .planId((String) document.getAttribute("planId"))
            .status(Node.Status.valueOf((String) document.getAttribute("status")))
            .parentId((String) document.getAttribute("parentId"))
            .build();
    }
}
