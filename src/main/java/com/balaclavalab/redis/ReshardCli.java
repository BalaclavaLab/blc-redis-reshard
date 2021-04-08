package com.balaclavalab.redis;

import io.lettuce.core.RedisURI;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;

public class ReshardCli {

    public static void main(String[] args) {
        Options options = new Options();
        options.addOption("f", "uriFrom", true, "Redis from (e.g. redis://localhost/1)");
        options.addOption("t", "uriTo", true, "Redis to (e.g. redis-cluster://localhost/0)");

        CommandLineParser commandLineParser = new DefaultParser();
        try {
            CommandLine commandLine = commandLineParser.parse(options, args);
            boolean hasOptions = commandLine.getOptions().length > 0;
            if (commandLine.hasOption("help") || !hasOptions) {
                printHelp(options);
            } else {
                RedisURI uriFrom = RedisURI.create(commandLine.getOptionValue("f"));
                RedisURI uriTo = RedisURI.create(commandLine.getOptionValue("t"));
                // TODO
            }
        } catch (ParseException e) {
            System.err.println("Parsing failed. Reason: " + e.getMessage());
            printHelp(options);
        }
    }

    private static void printHelp(Options options) {
        HelpFormatter formatter = new HelpFormatter();
        formatter.printHelp("blc-redis-reshard", "BLC Redis reshard utility", options, null, true);
    }
}