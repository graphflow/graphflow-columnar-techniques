package ca.waterloo.dsg.graphflow.runner.utils;

import org.apache.commons.cli.Option;

/**
 * The class containing all Command Line Options options needed by the runners.
 */
public class ArgsFactory {

    public static Option getHelpOption() {
        return new Option("h" /* HELP */, "help", false, "Print this message.");
    }

    /*
     * Serialize DataSet Runner:
     * ~~~~~~~~~~~~~~~~~~~~~~~~~
     *      INPUT_DIR               -i
     *      SERIALIZE_OUTPUT        -o
     */
    public static String INPUT_DIR = "i";
    public static String SERIALIZE_OUTPUT = "o";

    public static Option getInputDirOption() {
        var option = new Option(INPUT_DIR, "input_dir", true,
            "Absolute path to where the dataset is located.");
        option.setRequired(true);
        return option;
    }

    public static Option getOutputDirOption() {
        var option = new Option(SERIALIZE_OUTPUT, "output_dir", true,
            "Absolute path to serialize the input graph.");
        option.setRequired(true);
        return option;
    }

    /*
     * Benchmark Executor:
     * ~~~~~~~~~~~~~~~~~~~
     *      INPUT_DIR                -i
     *      BENCHMARK_QUERY          -q
     *      NUM_RUNS                 -r
     *      WARM_UP_RUNS             -w
     */
    public static String BENCHMARK_QUERY = "q";
    public static String NUM_RUNS = "r";
    public static String WARM_UP_RUNS = "w";

    public static Option getInputGraphDirectoryOption() {
        var option = new Option(INPUT_DIR, "input_dir", true,
            "Absolute path to the directory of the serialized input graph.");
        option.setRequired(true);
        return option;
    }

    public static Option getBenchmarkQueryOption() {
        var option = new Option(BENCHMARK_QUERY, "benchmark_dir", true,
            "Absolute path to the root of Benchmark directory");
        option.setRequired(true);
        return option;
    }

    public static Option getNumberRunsOptions() {
        var option = new Option(NUM_RUNS, "num_runs", true,
            "The number of runs of each query to be exectuted. Default is 2.");
        option.setRequired(false);
        return option;
    }

    public static Option getWarmupRunsOption() {
        var option = new Option(WARM_UP_RUNS, "warmup_runs", true,
            "The number of runs to be used for warming up the caches. Default is 1.");
        option.setRequired(false);
        return option;
    }
}
