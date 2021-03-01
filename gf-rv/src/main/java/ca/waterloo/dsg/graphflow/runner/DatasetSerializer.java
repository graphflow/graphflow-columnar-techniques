package ca.waterloo.dsg.graphflow.runner;

import ca.waterloo.dsg.graphflow.runner.utils.ArgsFactory;
import ca.waterloo.dsg.graphflow.runner.utils.DatasetMetadata;
import ca.waterloo.dsg.graphflow.storage.loader.GraphLoader;
import org.apache.commons.cli.Options;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class DatasetSerializer extends AbstractRunner {

    private static final Logger logger = LogManager.getLogger(DatasetSerializer.class);

    public static void main(String[] args) {
        // If the user asks for help, enforce it over the required options.
        if (isAskingHelp(args, getCommandLineOptions())) {
            return;
        }

        var cmdLine = parseCommandLine(args, getCommandLineOptions());
        if (null == cmdLine) {
            logger.info("could not parseQuery all the program arguments.");
            return;
        }
        var inputDirectory = cmdLine.getOptionValue(ArgsFactory.INPUT_DIR);
        try {
            var metadata = DatasetMetadata.readDatasetMetadata(inputDirectory);
            var outputDirectory = sanitizeDirStringAndMkdirIfNeeded(cmdLine.getOptionValue(
                ArgsFactory.SERIALIZE_OUTPUT));
            new GraphLoader().loadGraph(inputDirectory, metadata, outputDirectory);
        } catch (Exception e) {
            e.printStackTrace();
            logger.error("Error reading files: " + e.getMessage());
        }
    }

    private static Options getCommandLineOptions() {
        var options = new Options();
        options.addOption(ArgsFactory.getInputDirOption());         // INPUT_DIR            -i
        options.addOption(ArgsFactory.getOutputDirOption());        // SERIALIZE_OUTPUT     -o
        return options;
    }
}
