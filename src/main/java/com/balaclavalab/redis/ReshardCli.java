package com.balaclavalab.redis;

import io.lettuce.core.MigrateArgs;
import io.lettuce.core.RedisURI;
import io.lettuce.core.api.sync.RedisCommands;
import io.lettuce.core.cluster.RedisClusterClient;
import io.lettuce.core.cluster.api.sync.RedisAdvancedClusterCommands;
import io.lettuce.core.cluster.api.sync.RedisClusterCommands;
import io.lettuce.core.cluster.models.partitions.ClusterPartitionParser;
import io.lettuce.core.cluster.models.partitions.Partitions;
import io.lettuce.core.cluster.models.partitions.RedisClusterNode;
import io.lettuce.core.codec.ByteArrayCodec;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import static java.util.stream.Collectors.groupingBy;
import static java.util.stream.Collectors.toList;
import static java.util.stream.Collectors.toMap;
import static java.util.stream.Collectors.toUnmodifiableList;

public class ReshardCli {

    public static int REDIS_SLOT_COUNT = 16384;

    public static void main(String[] args) {
        Options options = new Options();
        options.addOption("u", "uri", true, "Redis to connect to (e.g. redis://localhost)");
        options.addOption("e", "excludeNodeIds", true, "Exclude node ids from balancing");
        options.addOption("a", "assign", false, "Perform unassigned slot assignment");
        options.addOption("r", "reshard", false, "Perform reshard");
        options.addOption("o", "orderForNodeIds", true, "Desired node order");
        options.addOption("ckis", "countkeysinslots", false, "Print number of keys in each slot");
        options.addOption("mb", "migrationBatchSize", true, "Migration batch size (default 1000)");
        options.addOption("y", "yes", false, "Do actual operations");
        options.addOption("t", "writeTestData", false, "Write test data to cluster (for testing)");
        options.addOption("tk", "testDataKeysCount", true, "How many test keys write to db (default 1000000, for testing)");
        options.addOption("dt", "deleteTestData", false, "Delete test data to cluster (for testing)");

        CommandLineParser commandLineParser = new DefaultParser();
        try {
            CommandLine commandLine = commandLineParser.parse(options, args);
            boolean hasOptions = commandLine.getOptions().length > 0;
            if (commandLine.hasOption("help") || !hasOptions) {
                printHelp(options);
            } else {
                RedisURI uri = RedisURI.create(commandLine.getOptionValue("u"));
                String excludeNodeIdsString = commandLine.getOptionValue("e");
                List<String> excludeNodeIds = excludeNodeIdsString == null
                        ? Collections.emptyList()
                        : List.of(excludeNodeIdsString.split(","));
                boolean assign = commandLine.hasOption("a");
                boolean reshard = commandLine.hasOption("r");
                String specifiedOrderForNodeIdsString = commandLine.getOptionValue("o");
                List<String> specifiedOrderForNodeIds = specifiedOrderForNodeIdsString == null
                        ? Collections.emptyList()
                        : List.of(specifiedOrderForNodeIdsString.split(","));
                boolean countKeysInSlot = commandLine.hasOption("ckis");
                int migrationBatchSize = Integer.parseInt(commandLine.getOptionValue("migrationBatchSize", "1000"));
                boolean commit = commandLine.hasOption("y");
                boolean writeTestData = commandLine.hasOption("t");
                int testDataKeysCount = Integer.parseInt(commandLine.getOptionValue("migrationBatchSize", "1000000"));
                boolean deleteTestData = commandLine.hasOption("dt");

                RedisAdvancedClusterCommands<byte[], byte[]> commands = RedisClusterClient.create(uri)
                        .connect(new ByteArrayCodec())
                        .sync();
                Partitions clusterPartitions = ClusterPartitionParser.parse(commands.clusterNodes());
                printClusterInformation(clusterPartitions);

                List<RedisClusterNode> clusterMasterNodes = clusterPartitions.stream()
                        .filter(clusterPartition -> clusterPartition.getRole().isMaster())
                        .collect(toUnmodifiableList());

                int nodeCount = (int) clusterPartitions.stream()
                        .filter(clusterPartition -> clusterPartition.getRole().isMaster())
                        .filter(clusterPartition -> !excludeNodeIds.contains(clusterPartition.getNodeId()))
                        .count();
                List<Object> clusterSlots = commands.clusterSlots();
                printClusterSlots(clusterSlots);

                List<List<Integer>> desiredSlots = createDesiredSlots(nodeCount);
                List<String> optimalNodeIds = specifiedOrderForNodeIds.isEmpty()
                        ? getOptimalNodeIds(clusterSlots, desiredSlots, clusterMasterNodes, excludeNodeIds)
                        : specifiedOrderForNodeIds;
                assert optimalNodeIds.size() == desiredSlots.size();
                printDesiredClusterSlots(desiredSlots, optimalNodeIds);

                Map<String, RedisCommands<byte[], byte[]>> nodeIdToClusterCommands = clusterPartitions.stream()
                        .map(clusterNode -> RedisClusterClient.create(clusterNode.getUri())
                                .connect(new ByteArrayCodec()).getConnection(clusterNode.getNodeId()).sync())
                        .collect(toMap(
                                RedisClusterCommands::clusterMyId,
                                clusterCommands -> clusterCommands));
                Map<String, RedisCommands<byte[], byte[]>> masterNodeIdToClusterCommands = clusterMasterNodes.stream()
                        .collect(toMap(
                                RedisClusterNode::getNodeId,
                                clusterMasterNode -> nodeIdToClusterCommands.get(clusterMasterNode.getNodeId())));

                Map<Integer, String> slotToDesiredNodeIdMap = new HashMap<>();
                for (int i = 0; i < desiredSlots.size(); i++) {
                    final int nodeNumber = i;
                    desiredSlots.get(i)
                            .forEach(slot -> slotToDesiredNodeIdMap.put(slot, optimalNodeIds.get(nodeNumber)));
                }

                if (countKeysInSlot) {
                    printCountKeysInSlot(clusterPartitions, nodeIdToClusterCommands);
                }

                if (assign) {
                    if (clusterSlots.isEmpty()) {
                        emptyClusterSlotAssignment(clusterMasterNodes, desiredSlots, nodeIdToClusterCommands, commit);

                        printCommitFlagMessage(commit);
                        System.exit(0);
                    }

                    checkIfAllSlotsAreAssigned(clusterPartitions, slotToDesiredNodeIdMap, nodeIdToClusterCommands, commit);
                }

                if (writeTestData && commit) {
                    writeTestData(commands, testDataKeysCount);
                }

                if (deleteTestData && commit) {
                    deleteTestData(commands, testDataKeysCount);
                }

                if (reshard) {
                    reshardSlots(clusterPartitions, slotToDesiredNodeIdMap, masterNodeIdToClusterCommands, migrationBatchSize, commit);
                }

                printCommitFlagMessage(commit);
            }
        } catch (ParseException e) {
            System.err.println("Parsing failed. Reason: " + e.getMessage());
            printHelp(options);
        }

        System.exit(0);
    }

    private static void printCommitFlagMessage(boolean commit) {
        if (!commit) {
            System.out.println("!!! Commit flag (--yes) was not set so this is just preview !!!");
        }
    }

    public static List<String> getOptimalNodeIds(
            List<Object> clusterSlots,
            List<List<Integer>> desiredSlots,
            List<RedisClusterNode> clusterMasterNodes,
            List<String> excludedNodeIds) {
        assert desiredSlots.size() == clusterMasterNodes.size();
        List<String> currentNodeIdOrder = getCurrentNodeIdOrder(clusterSlots);
        List<String> masterNodeIds = clusterMasterNodes.stream()
                .map(RedisClusterNode::getNodeId)
                .collect(toList());
        return Stream.concat(currentNodeIdOrder.stream(), masterNodeIds.stream())
                .filter(nodeId -> !excludedNodeIds.contains(nodeId))
                .distinct()
                .collect(toList());
    }

    @SuppressWarnings("rawtypes")
    public static List<String> getCurrentNodeIdOrder(List<Object> clusterSlots) {
        return clusterSlots.stream()
                .map(list -> (List) list)
                .sorted(Comparator.comparing(x -> ((Long) x.get(0))))
                .map(list -> list.get(2))
                .map(list -> ((List) list).get(2))
                .map(nodeIdBytes -> new String((byte[]) nodeIdBytes))
                .distinct()
                .collect(toUnmodifiableList());
    }

    private static void printClusterInformation(Partitions clusterPartitions) {
        System.out.println("Cluster partitions:");
        clusterPartitions.forEach(System.out::println);
        System.out.println();
    }

    @SuppressWarnings("rawtypes")
    private static void printClusterSlots(List<Object> clusterSlots) {
        System.out.println("Current cluster slots:");
        clusterSlots.stream()
                .map(list -> (List) list)
                .sorted(Comparator.comparing(x -> ((Long) x.get(0))))
                .forEach(list -> {
                    long from = (long) list.get(0);
                    long to = (long) list.get(1);
                    String nodeId = new String((byte[]) ((List) list.get(2)).get(2));
                    System.out.println("Slots " + from + "-" + to + " are on node " + nodeId);
                });
        if (clusterSlots.isEmpty()) {
            System.out.println("None\n");
        } else {
            System.out.println();
        }
    }

    private static void printDesiredClusterSlots(List<List<Integer>> desiredClusterSlots, List<String> optimalNodeIdOrder) {
        System.out.println("Desired cluster slots:");
        for (int i = 0; i < desiredClusterSlots.size(); i++) {
            List<Integer> slots = desiredClusterSlots.get(i);
            int from = slots.get(0);
            int to = slots.get(slots.size() - 1);
            String nodeId = optimalNodeIdOrder.get(i);
            System.out.println("Slots " + from + "-" + to + " should be on node " + nodeId);
        }

        System.out.println();
    }

    public static void emptyClusterSlotAssignment(
            List<RedisClusterNode> clusterMasterNodes,
            List<List<Integer>> desiredSlots,
            Map<String, RedisCommands<byte[], byte[]>> nodeIdToClusterCommands,
            boolean commit) {
        System.out.println("Redis cluster is empty, assigning desired slots...");
        for (int i = 0; i < clusterMasterNodes.size(); i++) {
            RedisClusterNode redisClusterNode = clusterMasterNodes.get(i);
            List<Integer> slotsForPartition = desiredSlots.get(i);
            int from = slotsForPartition.get(0);
            int to = slotsForPartition.get(slotsForPartition.size() - 1);
            System.out.println("Adding slots " + from + "-" + to + " for node " + redisClusterNode.getNodeId());
            if (commit) {
                RedisCommands<byte[], byte[]> commands = nodeIdToClusterCommands.get(redisClusterNode.getNodeId());
                commands.clusterAddSlots(slotsForPartition.stream().mapToInt(z -> z).toArray());
            }
        }
        System.out.println("Done\n");
    }

    private static void printCountKeysInSlot(
            Partitions clusterPartitions,
            Map<String, RedisCommands<byte[], byte[]>> nodeIdToClusterCommands) {
        System.out.println("Number of keys in each slot");
        IntStream.range(0, REDIS_SLOT_COUNT)
                .forEachOrdered(slot -> {
                    RedisClusterNode partitionBySlot = clusterPartitions.getPartitionBySlot(slot);
                    String nodeId = partitionBySlot.getNodeId();
                    long keys = nodeIdToClusterCommands.get(nodeId).clusterCountKeysInSlot(slot);
                    System.out.println("Slot " + slot + " has " + keys + " keys on node " + nodeId);
                    nodeIdToClusterCommands.entrySet().stream()
                            .filter(entry -> !entry.getKey().equals(nodeId))
                            .forEach(entry -> {
                                long keyInEntry = nodeIdToClusterCommands.get(entry.getKey()).clusterCountKeysInSlot(slot);
                                if (keyInEntry > 0) {
                                    System.out.println("  ... slot " + slot + " has " + keys + " keys on unassigned node " + nodeId);
                                }
                            });
                });
    }

    public static void checkIfAllSlotsAreAssigned(
            Partitions clusterPartitions,
            Map<Integer, String> slotToDesiredNodeIdMap,
            Map<String, RedisCommands<byte[], byte[]>> nodeIdToClusterCommands,
            boolean commit) {
        System.out.println("Checking if all slots are assigned...");
        IntStream.range(0, REDIS_SLOT_COUNT)
                .forEachOrdered(i -> {
                    RedisClusterNode partitionBySlot = clusterPartitions.getPartitionBySlot(i);
                    if (partitionBySlot == null) {
                        String desiredNodeId = slotToDesiredNodeIdMap.get(i);
                        System.out.println("Slot " + i + " is not assigned, will be assigned node " + desiredNodeId);
                        if (commit) {
                            nodeIdToClusterCommands.get(desiredNodeId).clusterAddSlots(i);
                        }
                    }
                });
        System.out.println("Done\n");
    }

    private static class ReshardAction {
        public String fromNodeId;
        public int slot;
        public String toNodeId;

        ReshardAction(String fromNodeId, int slot, String toNodeId) {
            this.fromNodeId = fromNodeId;
            this.slot = slot;
            this.toNodeId = toNodeId;
        }

        public String getFromNodeId() {
            return fromNodeId;
        }

        public int getSlot() {
            return slot;
        }

        public String getToNodeId() {
            return toNodeId;
        }
    }

    public static void reshardSlots(
            Partitions clusterPartitions,
            Map<Integer, String> slotToDesiredNodeIdMap,
            Map<String, RedisCommands<byte[], byte[]>> nodeIdToClusterCommands,
            int migrationBatchSize,
            boolean commit) {
        System.out.println("Checking if all slots are assigned to desired nodes...");

        List<ReshardAction> reshardActions = new ArrayList<>();
        IntStream.range(0, REDIS_SLOT_COUNT)
                .forEachOrdered(slot -> {
                    RedisClusterNode currentPartition = clusterPartitions.getPartitionBySlot(slot);
                    String desiredNodeId = slotToDesiredNodeIdMap.get(slot);
                    RedisClusterNode desiredPartition = clusterPartitions.getPartitionByNodeId(desiredNodeId);
                    if (currentPartition != desiredPartition) {
                        reshardActions.add(new ReshardAction(currentPartition.getNodeId(), slot, desiredNodeId));
                    }
                });

        if (reshardActions.size() == 0) {
            System.out.println("No actions needed. Done.\n");
            return;
        }

        Map<String, List<ReshardAction>> nodeIdToRemainingReshardActionsMap = reshardActions.stream()
                .collect(groupingBy(ReshardAction::getFromNodeId));
        ReshardAction currentAction = reshardActions.get(0);
        while (true) {
            String fromNodeId = currentAction.getFromNodeId();
            int slot = currentAction.getSlot();
            String toNodeId = currentAction.getToNodeId();
            moveSlot(clusterPartitions, slotToDesiredNodeIdMap, nodeIdToClusterCommands, slot, migrationBatchSize, commit);
            nodeIdToRemainingReshardActionsMap.get(fromNodeId).remove(currentAction);
            reshardActions.remove(currentAction);
            List<ReshardAction> remainingReshardActionsForToNodeId = nodeIdToRemainingReshardActionsMap.get(toNodeId);
            if (remainingReshardActionsForToNodeId != null && remainingReshardActionsForToNodeId.size() > 0) {
                currentAction = remainingReshardActionsForToNodeId.iterator().next();
            } else if (reshardActions.size() > 0) {
                currentAction = reshardActions.iterator().next();
            } else {
                break;
            }
        }
        System.out.println("Done\n");
    }

    private static void moveSlot(
            Partitions clusterPartitions,
            Map<Integer, String> slotToDesiredNodeIdMap,
            Map<String, RedisCommands<byte[], byte[]>> nodeIdToClusterCommands,
            int slot,
            int migrationBatchSize,
            boolean commit) {
        RedisClusterNode currentPartition = clusterPartitions.getPartitionBySlot(slot);
        String desiredNodeId = slotToDesiredNodeIdMap.get(slot);
        RedisClusterNode desiredPartition = clusterPartitions.getPartitionByNodeId(desiredNodeId);
        RedisURI desiredPartitionUri = desiredPartition.getUri();
        if (currentPartition != desiredPartition) {
            String currentNodeId = currentPartition.getNodeId();
            System.out.println("Slot " + slot + " is not on desired node, currently on " + currentNodeId + ", but should be on " + desiredNodeId);
            RedisCommands<byte[], byte[]> desiredClusterCommands = nodeIdToClusterCommands.get(desiredNodeId);
            RedisCommands<byte[], byte[]> currentClusterCommands = nodeIdToClusterCommands.get(currentNodeId);
            if (commit) {
                desiredClusterCommands.clusterSetSlotImporting(slot, currentNodeId);
                currentClusterCommands.clusterSetSlotMigrating(slot, desiredNodeId);
            }

            System.out.println("Moving keys in slot " + slot + " to new node, total key count: " + currentClusterCommands.clusterCountKeysInSlot(slot));
            if (commit) {
                List<byte[]> keys = currentClusterCommands.clusterGetKeysInSlot(slot, migrationBatchSize);
                while (!keys.isEmpty()) {
                    System.out.println("Moving keys in slot " + slot + " to new node, key count: " + keys.size());
                    currentClusterCommands.migrate(desiredPartitionUri.getHost(), desiredPartitionUri.getPort(), 0, 60_000, MigrateArgs.Builder.keys(keys).replace());
                    keys = currentClusterCommands.clusterGetKeysInSlot(slot, migrationBatchSize);
                }

                desiredClusterCommands.clusterSetSlotNode(slot, desiredNodeId);
                currentClusterCommands.clusterSetSlotNode(slot, desiredNodeId);
                nodeIdToClusterCommands.values()
                        .forEach(clusterCommands -> clusterCommands.clusterSetSlotNode(slot, desiredNodeId));
                // desiredClusterCommands.clusterSetSlotStable(slot);
            }
        }
    }

    private static List<List<Integer>> createDesiredSlots(int nodeCount) {
        int slotCountPerNode = REDIS_SLOT_COUNT / nodeCount;
        return IntStream.range(0, nodeCount)
                .boxed()
                .map(nodeNumber ->
                        IntStream.range(
                                        slotCountPerNode * nodeNumber,
                                        nodeNumber == (nodeCount - 1) ? REDIS_SLOT_COUNT : slotCountPerNode * (nodeNumber + 1))
                                .boxed().collect(toList()))
                .collect(toList());
    }

    private static void writeTestData(RedisAdvancedClusterCommands<byte[], byte[]> commands, int count) {
        System.out.println("Writing test data...");
        IntStream.range(0, count)
                .boxed()
                .forEach(i ->
                        commands.set(String.valueOf(i).getBytes(), String.valueOf(i).getBytes()));
        System.out.println("Done\n");
    }

    private static void deleteTestData(RedisAdvancedClusterCommands<byte[], byte[]> commands, int count) {
        System.out.println("Deleting test data...");
        IntStream.range(0, count)
                .boxed()
                .forEach(i ->
                        commands.del(String.valueOf(i).getBytes()));
        System.out.println("Done\n");
    }

    private static void printHelp(Options options) {
        HelpFormatter formatter = new HelpFormatter();
        formatter.printHelp("blc-redis-reshard", "BLC Redis reshard utility", options, null, true);
    }
}
