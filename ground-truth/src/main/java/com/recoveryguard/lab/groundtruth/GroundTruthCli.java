package com.recoveryguard.lab.groundtruth;

import com.fasterxml.jackson.databind.node.ObjectNode;

import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

public final class GroundTruthCli {
    private GroundTruthCli() {
    }

    public static void main(String[] args) {
        int exitCode;
        try {
            exitCode = execute(args);
        } catch (Exception exception) {
            System.err.println("ERROR: " + exception.getMessage());
            exitCode = 2;
        }
        if (exitCode != 0) {
            System.exit(exitCode);
        }
    }

    static int execute(String[] args) throws Exception {
        if (args.length == 0 || args[0].equals("help") || args[0].equals("--help")) {
            printHelp();
            return 0;
        }

        String command = args[0];
        Map<String, String> options = parseOptions(args);
        validateOptions(command, options);
        Path outputDirectory = Path.of(options.getOrDefault("output-dir", "runs"));
        RunStore runStore = new RunStore(outputDirectory);

        switch (command) {
            case "create" -> {
                require(options, "topic", "bootstrap-servers");
                RunStore.Run run = runStore.create(
                        options.get("run-id"),
                        options.get("topic"),
                        options.get("bootstrap-servers"),
                        options.getOrDefault("description", "")
                );
                ObjectNode result = JsonSupport.MAPPER.createObjectNode();
                result.put("status", "CREATED");
                result.put("runId", run.runId());
                result.put("runDirectory", run.directory().toString());
                result.put("ledger", run.ledger().path().toString());
                printJson(result);
                return 0;
            }
            case "produce" -> {
                require(options, "run-id", "count");
                RunStore.Run run = runStore.load(options.get("run-id"));
                int count = parsePositiveInt(options.get("count"), "count");
                int payloadBytes = parsePositiveInt(options.getOrDefault("payload-bytes", "256"), "payload-bytes");
                long intervalMs = Long.parseLong(options.getOrDefault("interval-ms", "0"));
                int confirmed = new WorkloadProducer().produce(run, count, payloadBytes, intervalMs);
                ObjectNode result = JsonSupport.MAPPER.createObjectNode();
                result.put("runId", run.runId());
                result.put("requested", count);
                result.put("confirmed", confirmed);
                result.put("otherOutcomes", count - confirmed);
                printJson(result);
                return confirmed == count ? 0 : 1;
            }
            case "failure-start" -> {
                require(options, "run-id", "failure-id", "target", "action");
                RunStore.Run run = runStore.load(options.get("run-id"));
                new FailureRecorder().start(
                        run,
                        options.get("failure-id"),
                        options.get("target"),
                        options.get("action"),
                        options.getOrDefault("details", "")
                );
                System.out.println("Failure start recorded.");
                return 0;
            }
            case "failure-end" -> {
                require(options, "run-id", "failure-id", "result");
                RunStore.Run run = runStore.load(options.get("run-id"));
                new FailureRecorder().end(
                        run,
                        options.get("failure-id"),
                        options.get("result"),
                        options.getOrDefault("details", "")
                );
                System.out.println("Failure end recorded.");
                return 0;
            }
            case "verify" -> {
                require(options, "run-id");
                RunStore.Run run = runStore.load(options.get("run-id"));
                String defaultReportName = run.runId() + "-verification-" + UUID.randomUUID() + ".json";
                Path report = Path.of(options.getOrDefault(
                        "report", "reports/" + defaultReportName));
                long timeoutMs = parsePositiveLong(options.getOrDefault("timeout-ms", "60000"), "timeout-ms");
                RecordVerifier.VerificationResult result =
                        new RecordVerifier().verify(run, report, timeoutMs);
                printJson(result.report());
                System.out.println("Report written to " + report.toAbsolutePath().normalize());
                return result.status().equals("PASS") ? 0 : 1;
            }
            case "show-ledger" -> {
                require(options, "run-id");
                RunStore.Run run = runStore.load(options.get("run-id"));
                ObjectNode result = JsonSupport.MAPPER.createObjectNode();
                result.put("runId", run.runId());
                result.put("ledger", run.ledger().path().toString());
                result.set("events", JsonSupport.MAPPER.valueToTree(run.ledger().readAll()));
                printJson(result);
                return 0;
            }
            default -> {
                throw new IllegalArgumentException("Unknown command: " + command);
            }
        }
    }

    private static void validateOptions(String command, Map<String, String> options) {
        Map<String, java.util.Set<String>> supported = Map.of(
                "create", java.util.Set.of("topic", "bootstrap-servers", "output-dir", "run-id", "description"),
                "produce", java.util.Set.of("run-id", "count", "output-dir", "payload-bytes", "interval-ms"),
                "failure-start", java.util.Set.of("run-id", "failure-id", "target", "action", "output-dir", "details"),
                "failure-end", java.util.Set.of("run-id", "failure-id", "result", "output-dir", "details"),
                "verify", java.util.Set.of("run-id", "output-dir", "report", "timeout-ms"),
                "show-ledger", java.util.Set.of("run-id", "output-dir")
        );
        java.util.Set<String> allowed = supported.get(command);
        if (allowed == null) {
            throw new IllegalArgumentException("Unknown command: " + command);
        }
        for (String option : options.keySet()) {
            if (!allowed.contains(option)) {
                throw new IllegalArgumentException("Unsupported option for " + command + ": --" + option);
            }
        }
    }

    private static Map<String, String> parseOptions(String[] args) {
        Map<String, String> options = new HashMap<>();
        for (int index = 1; index < args.length; index++) {
            String token = args[index];
            if (!token.startsWith("--") || token.length() == 2) {
                throw new IllegalArgumentException("Expected an option beginning with --, received: " + token);
            }
            String key = token.substring(2);
            if (index + 1 >= args.length || args[index + 1].startsWith("--")) {
                throw new IllegalArgumentException("Missing value for --" + key);
            }
            if (options.putIfAbsent(key, args[++index]) != null) {
                throw new IllegalArgumentException("Option repeated: --" + key);
            }
        }
        return options;
    }

    private static void require(Map<String, String> options, String... keys) {
        for (String key : keys) {
            if (options.get(key) == null || options.get(key).isBlank()) {
                throw new IllegalArgumentException("Required option missing: --" + key);
            }
        }
    }

    private static int parsePositiveInt(String value, String field) {
        try {
            int parsed = Integer.parseInt(value);
            if (parsed < 1) {
                throw new IllegalArgumentException(field + " must be greater than zero");
            }
            return parsed;
        } catch (NumberFormatException exception) {
            throw new IllegalArgumentException(field + " must be an integer", exception);
        }
    }

    private static long parsePositiveLong(String value, String field) {
        try {
            long parsed = Long.parseLong(value);
            if (parsed < 1) {
                throw new IllegalArgumentException(field + " must be greater than zero");
            }
            return parsed;
        } catch (NumberFormatException exception) {
            throw new IllegalArgumentException(field + " must be an integer", exception);
        }
    }

    private static void printJson(ObjectNode node) throws Exception {
        System.out.println(JsonSupport.MAPPER.writerWithDefaultPrettyPrinter().writeValueAsString(node));
    }

    private static void printHelp() {
        System.out.println("""
                RecoveryGuard Lab Ground Truth

                Commands:
                  create --topic TOPIC --bootstrap-servers HOSTS [--output-dir DIR]
                         [--run-id ID] [--description TEXT]
                  produce --run-id ID --count N [--output-dir DIR] [--payload-bytes N]\n                          [--interval-ms N]
                  failure-start --run-id ID --failure-id ID --target NODE --action ACTION
                               [--output-dir DIR] [--details TEXT]
                  failure-end --run-id ID --failure-id ID --result RESULT
                             [--output-dir DIR] [--details TEXT]
                  verify --run-id ID [--output-dir DIR] [--report FILE] [--timeout-ms N]
                  show-ledger --run-id ID [--output-dir DIR]
                  help

                Defaults:
                  output directory: runs
                  payload size: 256 bytes
                  verification timeout: 60000 ms

                Producer settings:
                  acks=all, enable.idempotence=true, retries=Integer.MAX_VALUE

                Verification:
                  manually assigns topic partitions, disables auto-commit, and does not join a group.
                """);
    }
}
