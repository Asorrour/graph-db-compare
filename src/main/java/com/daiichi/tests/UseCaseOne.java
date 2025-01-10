package com.daiichi.tests;

import com.daiichi.ArangoDBWrapper;
import com.daiichi.DBWrapper;
import com.daiichi.NeptuneBWrapper;
import com.daiichi.OrientDBWrapper;
import com.daiichi.pojo.ExecutionPlan;
import com.daiichi.pojo.Node;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.context.event.ApplicationStartedEvent;
import org.springframework.context.event.EventListener;
import org.springframework.stereotype.Service;

import java.time.Duration;
import java.time.Instant;
import java.util.*;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static com.daiichi.pojo.Constants.TEST_GRAPH_COUNT;
import static com.daiichi.pojo.Constants.TEST_GRAPH_DEPTH;

@Slf4j
@Service
@RequiredArgsConstructor
class UseCaseOne {

    private final ArangoDBWrapper arangoDBWrapper;
    private final OrientDBWrapper orientDBWrapper;
    private final NeptuneBWrapper neptuneBWrapper;

    @EventListener(ApplicationStartedEvent.class)
    public void execute() {
        log.info("Starting Use Case One Benchmark");
        runUseCaseForDb(DBWrapper.DB.ARGANO);
        runUseCaseForDb(DBWrapper.DB.ORIENT);
        runUseCaseForDb(DBWrapper.DB.NEPTUNE);

//        OrientDb
//        Count: 500, Min: 37, Max: 81, Average: 39.72
//        Creation Stats, Count: 100, Min: 13, Max: 30, Average: 19.58
//        Update Stats, Count: 500, Min: 36, Max: 71, Average: 39.2
//        Creation Stats, Count: 100, Min: 77, Max: 182, Average: 88.76
//        Update Stats, Count: 500, Min: 38, Max: 89, Average: 40.434
//        Creation Stats, Count: 100, Min: 14, Max: 30, Average: 20.84
//        Update Stats, Count: 500, Min: 38, Max: 73, Average: 40.662
//        Creation Stats, Count: 100, Min: 77, Max: 204, Average: 88.37
//        Update Stats, Count: 500, Min: 37, Max: 92, Average: 39.89

//        ArangoDb
//        Creation Stats, Count: 100, Min: 129, Max: 363, Average: 155.1
//        Update Stats, Count: 500, Min: 1, Max: 43, Average: 2.294
//        Creation Stats, Count: 100, Min: 4, Max: 47, Average: 6.18
//        Update Stats, Count: 500, Min: 1, Max: 45, Average: 2.87
//        Creation Stats, Count: 100, Min: 4, Max: 48, Average: 6.23
//        Update Stats, Count: 500, Min: 5, Max: 44, Average: 18.824  (parallel)
//        Creation Stats, Count: 100, Min: 4, Max: 47, Average: 6.17
//        Update Stats, Count: 500, Min: 5, Max: 46, Average: 18.916
//        Creation Stats, Count: 100, Min: 132, Max: 374, Average: 155.8
//        Update Stats, Count: 500, Min: 3, Max: 44, Average: 13.674

//        Neptune
//        Creation Stats, Count: 100, Min: 240, Max: 2489, Average: 508.17
//        Update Stats, Count: 500, Min: 3, Max: 1128, Average: 11.176
//        Creation Stats, Count: 100, Min: 6, Max: 93, Average: 11.63
//        Update Stats, Count: 500, Min: 3, Max: 143, Average: 6.676
//        Creation Stats, Count: 100, Min: 6, Max: 108, Average: 11.81
//        Update Stats, Count: 500, Min: 2, Max: 98, Average: 5.318
//        Creation Stats, Count: 100, Min: 5, Max: 104, Average: 11.58
//        Update Stats, Count: 500, Min: 2, Max: 129, Average: 5.872
//        Creation Stats, Count: 100, Min: 5, Max: 74, Average: 10.31
//        Update Stats, Count: 500, Min: 3, Max: 101, Average: 5.35
//        Creation Stats, Count: 100, Min: 5, Max: 100, Average: 9.56
//        Update Stats, Count: 500, Min: 3, Max: 104, Average: 5.336

        log.info("Use Case One Benchmark Complete");
    }

    private void runUseCaseForDb(DBWrapper.DB dbType) {
        DBWrapper dbWrapper = getDbWrapper(dbType);

        // Step 1: Create n graphs (trees) with depth 3 in both databases
        Map<String, ExecutionPlan> graphs = IntStream.range(0, TEST_GRAPH_COUNT)
            .mapToObj(this::createExecutionPlan)
            .collect(Collectors.toMap(ExecutionPlan::getId, Function.identity()));

        IntSummaryStatistics creationStats = graphs.values().stream().mapToInt(plan -> {
            Instant startTime = Instant.now();
            String treeId = plan.getId();
            log.debug("Storing Plan: {}", treeId);
            dbWrapper.createTree(plan);
            return (int) Duration.between(startTime, Instant.now()).toMillis();
        }).summaryStatistics();

        // Step 2: Fetch a random graph, update one node, and save
        List<String> planIds = new ArrayList<>(graphs.keySet());
        IntSummaryStatistics updateStats = IntStream.range(1, 501).map(i -> {
            Instant startTime = Instant.now();
            int randomIndex = new Random().nextInt(planIds.size());
            String planId = planIds.get(randomIndex);
            log.info("Run number: {}: Updating plan: {}", i, planId);
            dbWrapper.fetchAndUpdate(planId);
            return (int) Duration.between(startTime, Instant.now()).toMillis();
        }).summaryStatistics();

        log.info("{}: Creation Stats, Count: {}, Min: {}, Max: {}, Average: {}",
            dbType, creationStats.getCount(), creationStats.getMin(), creationStats.getMax(), creationStats.getAverage());
        log.info("{}: Update Stats, Count: {}, Min: {}, Max: {}, Average: {}",
            dbType, updateStats.getCount(), updateStats.getMin(), updateStats.getMax(), updateStats.getAverage());
    }

    private ExecutionPlan createExecutionPlan(int graphId) {
        List<Node> nodes = new ArrayList<>();
        String planId = "Plan %s".formatted(graphId);
        Node root = Node.builder()
            .id(UUID.randomUUID().toString())
            .planId(planId)
            .data("Root Node for %s".formatted(planId))
            .build();
        nodes.add(root);

        createExecutionPlan(nodes, root, 0);
        log.info("Graph {} created with {} nodes", graphId, nodes.size());
        return ExecutionPlan.builder()
            .id(planId)
            .graph(nodes)
            .build();
    }

    private void createExecutionPlan(List<Node> nodes, Node parent, int level) {
        if (level >= TEST_GRAPH_DEPTH) return;

        for (int i = 0; i < 3; i++) {
            String planId = parent.getPlanId();
            Node child = Node.builder()
                .id(UUID.randomUUID().toString())
                .planId(planId)
                .data("Child %d-%d of %s".formatted(level + 1, i + 1, planId))
                .parentId(parent.getId())
                .build();
            nodes.add(child);
            createExecutionPlan(nodes, child, level + 1);
        }
    }

    private DBWrapper getDbWrapper(DBWrapper.DB db) {
        switch (db) {
            case ARGANO -> {
                return arangoDBWrapper;
            }
            case ORIENT -> {
                return orientDBWrapper;
            }
            case NEPTUNE -> {
                return neptuneBWrapper;
            }
        }
        throw new RuntimeException("DB isn't supported");
    }
}
