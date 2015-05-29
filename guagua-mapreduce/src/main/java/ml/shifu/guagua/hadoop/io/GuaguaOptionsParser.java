/*
 * Copyright [2013-2014] PayPal Software Foundation
 *  
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *  
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package ml.shifu.guagua.hadoop.io;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.PrintStream;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Copy from hadoop GenericOptionsParser and extend it.
 * 
 * <p>
 * Add other parameter support like -w, -m ...
 * 
 * @see Tool
 * @see ToolRunner
 */
public class GuaguaOptionsParser {

    private static final Logger LOG = LoggerFactory.getLogger(GuaguaOptionsParser.class);

    private static final String FILE_SEPERATOR = ",";
    private Configuration conf;
    private CommandLine commandLine;

    /**
     * Create an options parser with the given options to parse the args.
     * 
     * @param opts
     *            the options
     * @param args
     *            the command line arguments
     * @throws IOException
     */
    public GuaguaOptionsParser(Options opts, String[] args) throws IOException {
        this(new Configuration(), new Options(), args);
    }

    /**
     * Create an options parser to parse the args.
     * 
     * @param args
     *            the command line arguments
     * @throws IOException
     */
    public GuaguaOptionsParser(String[] args) throws IOException {
        this(new Configuration(), new Options(), args);
    }

    /**
     * Create a <code>GuaguaOptionsParser<code> to parse only the generic Hadoop arguments. 
     * 
     * The array of string arguments other than the generic arguments can be obtained by {@link #getRemainingArgs()}.
     * 
     * @param conf
     *            the <code>Configuration</code> to modify.
     * @param args
     *            command-line arguments.
     * @throws IOException
     */
    public GuaguaOptionsParser(Configuration conf, String[] args) throws IOException {
        this(conf, new Options(), args);
    }

    /**
     * Create a <code>GuaguaOptionsParser</code> to parse given options as well as generic Hadoop options.
     * 
     * The resulting <code>CommandLine</code> object can be obtained by {@link #getCommandLine()}.
     * 
     * @param conf
     *            the configuration to modify
     * @param options
     *            options built by the caller
     * @param args
     *            User-specified arguments
     * @throws IOException
     */
    public GuaguaOptionsParser(Configuration conf, Options options, String[] args) throws IOException {
        parseGeneralOptions(options, conf, args);
        this.conf = conf;
    }

    /**
     * Returns an array of Strings containing only application-specific arguments.
     * 
     * @return array of <code>String</code>s containing the un-parsed arguments or <strong>empty array</strong> if
     *         commandLine was not defined.
     */
    public String[] getRemainingArgs() {
        return (commandLine == null) ? new String[] {} : commandLine.getArgs();
    }

    /**
     * Get the modified configuration
     * 
     * @return the configuration that has the modified parameters.
     */
    public Configuration getConfiguration() {
        return conf;
    }

    /**
     * Returns the commons-cli <code>CommandLine</code> object to process the parsed arguments.
     * 
     * Note: If the object is created with {@link #GuaguaOptionsParser(Configuration, String[])}, then returned object
     * will only contain parsed generic options.
     * 
     * @return <code>CommandLine</code> representing list of arguments parsed against Options descriptor.
     */
    public CommandLine getCommandLine() {
        return commandLine;
    }

    /**
     * Specify properties of each generic option
     */
    @SuppressWarnings("static-access")
    private static Options buildGeneralOptions(Options opts) {
        Option fs = OptionBuilder.withArgName("local|namenode:port").hasArg().withDescription("specify a namenode")
                .create("fs");
        Option jt = OptionBuilder.withArgName("local|jobtracker:port").hasArg()
                .withDescription("specify a job tracker").create("jt");
        Option oconf = OptionBuilder.withArgName("configuration file").hasArg()
                .withDescription("specify an application configuration file").create("conf");
        Option property = OptionBuilder.withArgName("property=value").hasArg()
                .withDescription("use value for given property").create('D');
        Option libjars = OptionBuilder.withArgName("paths").hasArg()
                .withDescription("comma separated jar files to include in the classpath.").create("libjars");
        Option files = OptionBuilder.withArgName("paths").hasArg()
                .withDescription("comma separated files to be copied to the " + "map reduce cluster").create("files");
        Option archives = OptionBuilder.withArgName("paths").hasArg()
                .withDescription("comma separated archives to be unarchived" + " on the compute machines.")
                .create("archives");
        // file with security tokens
        Option tokensFile = OptionBuilder.withArgName("tokensFile").hasArg()
                .withDescription("name of the file with the tokens").create("tokenCacheFile");
        Option input = OptionBuilder.withArgName("paths").hasArg().withDescription("specify input folder").create("i");
        Option zk = OptionBuilder.withArgName("zkserverhost:port,zkserverhost:port").hasArg()
                .withDescription("specify zookeeper servers").create("z");
        Option worker = OptionBuilder.withArgName("class name").hasArg().withDescription("specify worker class name")
                .create("w");
        Option master = OptionBuilder.withArgName("class name").hasArg().withDescription("specify master class name")
                .create("m");
        Option masterResult = OptionBuilder.withArgName("class name").hasArg()
                .withDescription("specify master result class name").create("mr");
        Option workerResult = OptionBuilder.withArgName("class name").hasArg()
                .withDescription("specify worker result class name").create("wr");
        Option iteration = OptionBuilder.withArgName("1").hasArg().withDescription("specify iteration count")
                .create("c");
        Option name = OptionBuilder.withArgName("job name").hasArg().withDescription("specify job name").create("n");
        Option inputformat = OptionBuilder.withArgName("class name").hasArg()
                .withDescription("specify input format class name").create("inputformat");

        opts.addOption(fs);
        opts.addOption(jt);
        opts.addOption(oconf);
        opts.addOption(property);
        opts.addOption(libjars);
        opts.addOption(files);
        opts.addOption(archives);
        opts.addOption(tokensFile);

        opts.addOption(input);
        opts.addOption(zk);
        opts.addOption(worker);
        opts.addOption(master);
        opts.addOption(masterResult);
        opts.addOption(workerResult);
        opts.addOption(iteration);
        opts.addOption(name);
        opts.addOption(inputformat);

        return opts;
    }

    /**
     * Modify configuration according user-specified generic options
     * 
     * @param conf
     *            Configuration to be modified
     * @param line
     *            User-specified generic options
     */
    private void processGeneralOptions(Configuration conf, CommandLine line) throws IOException {
        if(line.hasOption("fs")) {
            FileSystem.setDefaultUri(conf, line.getOptionValue("fs"));
        }

        if(line.hasOption("jt")) {
            conf.set("mapred.job.tracker", line.getOptionValue("jt"));
        }
        if(line.hasOption("conf")) {
            String[] values = line.getOptionValues("conf");
            for(String value: values) {
                conf.addResource(new Path(value));
            }
        }
        if(line.hasOption("libjars")) {
            conf.set("tmpjars", validateFiles(line.getOptionValue("libjars"), conf));
            // setting libjars in client classpath
            URL[] libjars = getLibJars(conf);
            if(libjars != null && libjars.length > 0) {
                conf.setClassLoader(new URLClassLoader(libjars, conf.getClassLoader()));
                Thread.currentThread().setContextClassLoader(
                        new URLClassLoader(libjars, Thread.currentThread().getContextClassLoader()));
            }
        }
        if(line.hasOption("files")) {
            conf.set("tmpfiles", validateFiles(line.getOptionValue("files"), conf));
        }
        if(line.hasOption("archives")) {
            conf.set("tmparchives", validateFiles(line.getOptionValue("archives"), conf));
        }
        if(line.hasOption('D')) {
            String[] property = line.getOptionValues('D');
            for(String prop: property) {
                String[] keyval = prop.split("=", 2);
                if(keyval.length == 2) {
                    conf.set(keyval[0], keyval[1]);
                }
            }
        }
        conf.setBoolean("mapred.used.genericoptionsparser", true);

        // tokensFile
        if(line.hasOption("tokenCacheFile")) {
            String fileName = line.getOptionValue("tokenCacheFile");
            // check if the local file exists
            try {
                FileSystem localFs = FileSystem.getLocal(conf);
                Path p = new Path(fileName);
                if(!localFs.exists(p)) {
                    throw new FileNotFoundException("File " + fileName + " does not exist.");
                }

                LOG.debug("setting conf tokensFile: {}", fileName);
                conf.set("mapreduce.job.credentials.json", localFs.makeQualified(p).toString());
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }
    }

    /**
     * If libjars are set in the conf, parse the libjars.
     */
    public static URL[] getLibJars(Configuration conf) throws IOException {
        String jars = conf.get("tmpjars");
        if(jars == null) {
            return null;
        }
        String[] files = jars.split(FILE_SEPERATOR);
        List<URL> cp = new ArrayList<URL>();
        for(String file: files) {
            Path tmp = new Path(file);
            if(tmp.getFileSystem(conf).equals(FileSystem.getLocal(conf))) {
                cp.add(FileSystem.getLocal(conf).pathToFile(tmp).toURI().toURL());
            }
        }
        return cp.toArray(new URL[0]);
    }

    /**
     * Take input as a comma separated list of files and verifies if they exist. It defaults for file:/// if the files
     * specified do not have a scheme. it returns the paths uri converted defaulting to file:///. So an input of
     * /home/user/file1,/home/user/file2 would return file:///home/user/file1,file:///home/user/file2
     */
    private String validateFiles(String files, Configuration conf) throws IOException {
        if(files == null)
            return null;
        String[] fileArr = files.split(FILE_SEPERATOR);
        String[] finalArr = new String[fileArr.length];
        for(int i = 0; i < fileArr.length; i++) {
            String tmp = fileArr[i];
            String finalPath;
            URI pathURI;
            try {
                pathURI = new URI(tmp);
            } catch (URISyntaxException e) {
                throw new IllegalArgumentException(e);
            }
            Path path = new Path(pathURI.toString());
            FileSystem localFs = FileSystem.getLocal(conf);
            if(pathURI.getScheme() == null) {
                // default to the local file system
                // check if the file exists or not first
                if(!localFs.exists(path)) {
                    throw new FileNotFoundException("File " + tmp + " does not exist.");
                }
                finalPath = path.makeQualified(localFs).toString();
            } else {
                // check if the file exists in this file system
                // we need to recreate this filesystem object to copy
                // these files to the file system jobtracker is running
                // on.
                FileSystem fs = path.getFileSystem(conf);
                if(!fs.exists(path)) {
                    throw new FileNotFoundException("File " + tmp + " does not exist.");
                }
                finalPath = path.makeQualified(fs).toString();
            }
            finalArr[i] = finalPath;
        }
        return StringUtils.arrayToString(finalArr);
    }

    /**
     * Parse the user-specified options, get the generic options, and modify
     * configuration accordingly
     * 
     * @param conf
     *            Configuration to be modified
     * @param args
     *            User-specified arguments
     * @return Command-specific arguments
     */
    private String[] parseGeneralOptions(Options opts, Configuration conf, String[] args) throws IOException {
        opts = buildGeneralOptions(opts);
        CommandLineParser parser = new GnuParser();
        try {
            commandLine = parser.parse(opts, args, true);
            processGeneralOptions(conf, commandLine);
            return commandLine.getArgs();
        } catch (ParseException e) {
            LOG.warn("options parsing failed: {}", e.getMessage());

            HelpFormatter formatter = new HelpFormatter();
            formatter.printHelp("general options are: ", opts);
        }
        return args;
    }

    /**
     * Print the usage message for generic command-line options supported.
     * 
     * @param out
     *            stream to print the usage message to.
     */
    public static void printGenericCommandUsage(PrintStream out) {
        out.println("Generic options supported are");
        out.println("-conf <configuration file>     specify an application configuration file");
        out.println("-D <property=value>            use value for given property");
        out.println("-fs <local|namenode:port>      specify a namenode");
        out.println("-jt <local|jobtracker:port>    specify a job tracker");
        out.println("-files <comma separated list of files>    specify comma separated files to be copied to the map reduce cluster");
        out.println("-libjars <comma separated list of jars>    specify comma separated jar files to include in the classpath.");
        out.println("-archives <comma separated list of archives>    specify comma separated archives to be unarchived on the compute machines.");
        out.println("-i <input folder or input file>    specify input folder or input file.");
        out.println("-z <comma separated list of zookeeper servers>    specify zookeeper servers.");
        out.println("-w <full qualified class name>    specify worker class name.");
        out.println("-m <full qualified class name>    specify master class name.");
        out.println("-mr <full qualified class name>    specify master result class name.");
        out.println("-wr <full qualified class name>    specify worker result class name.");
        out.println("-c <number>    specify maximal iteration count.");
        out.println("-n <job name>    specify job name.");
        out.println("-inputformat <inputformat class name>    specify inputformat class name.\n");
        out.println("The general command line syntax is");
        out.println("bin/hadoop command [genericOptions] [commandOptions]\n");
    }

}
