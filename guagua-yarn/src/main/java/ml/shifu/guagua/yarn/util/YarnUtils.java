/**
 * Copyright [2013-2014] eBay Software Foundation
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
package ml.shifu.guagua.yarn.util;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import ml.shifu.guagua.yarn.GuaguaYarnConstants;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.MRJobConfig;
import org.apache.hadoop.yarn.api.ApplicationConstants;
import org.apache.hadoop.yarn.api.ApplicationConstants.Environment;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.LocalResource;
import org.apache.hadoop.yarn.api.records.LocalResourceType;
import org.apache.hadoop.yarn.api.records.LocalResourceVisibility;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.util.ConverterUtils;
import org.apache.hadoop.yarn.util.Records;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Splitter;
import com.google.common.collect.Maps;

/**
 * {@link YarnUtils} is yarn-related helper class.
 */
public final class YarnUtils {

    private static final Logger LOG = LoggerFactory.getLogger(YarnUtils.class);

    public static final String GUAGUA_YARN_TMP = "tmp";

    /** Private constructor, this is a utility class only */
    private YarnUtils() {
    }

    /**
     * Build local resources including main app jar, lib jars, log4j.properties and guagua-conf.xml.
     */
    public static Map<String, LocalResource> getLocalResourceMap(Configuration conf, ApplicationId appId)
            throws IOException {
        Map<String, LocalResource> localResources = Maps.newHashMap();

        FileSystem fs = FileSystem.get(conf);

        // yarn app jar
        addFileToResourceMap(localResources, fs,
                getPathForResource(fs, conf.get(GuaguaYarnConstants.GUAGUA_YARN_APP_JAR), appId));
        //
        try {
            addFileToResourceMap(localResources, fs,
                    getPathForResource(fs, GuaguaYarnConstants.GUAGUA_LOG4J_PROPERTIES, appId));
        } catch (FileNotFoundException e) {
            LOG.warn("log4j.properties does not exist!");
        }

        addFileToResourceMap(localResources, fs, getPathForResource(fs, GuaguaYarnConstants.GUAGUA_CONF_FILE, appId));

        // Libs
        String libs = conf.get(GuaguaYarnConstants.GUAGUA_YARN_APP_LIB_JAR);
        if(StringUtils.isNotEmpty(libs)) {
            for(String jar: Splitter.on(GuaguaYarnConstants.GUAGUA_APP_LIBS_SEPERATOR).split(libs)) {
                addFileToResourceMap(localResources, fs, getPathForResource(fs, jar.trim(), appId));
            }
        }

        return localResources;
    }

    /**
     * Return java command according to main class, vm args, program args and memory settings.
     */
    public static List<String> getCommand(String mainClass, String vmArgs, String programArgs, String memory) {
        List<String> commands = new ArrayList<String>(1);
        commands.add(getCommandBase(mainClass, vmArgs, programArgs, memory).toString());
        LOG.info("commands:{}", commands);
        return commands;
    }

    private static StringBuilder getCommandBase(String mainClass, String vmArgs, String programArgs, String memory) {
        List<String> commands = new ArrayList<String>(8);
        commands.add("exec");
        commands.add(Environment.JAVA_HOME.$() + File.separator + "bin" + File.separator + "java");
        commands.add("-Xms" + memory + "m");
        commands.add("-Xmx" + memory + "m");
        if(vmArgs != null) {
            commands.add(vmArgs);
        }
        commands.add("-cp .:${CLASSPATH}");
        commands.add(mainClass);
        if(programArgs != null) {
            commands.add(programArgs);
        }
        commands.add("1> " + ApplicationConstants.LOG_DIR_EXPANSION_VAR + File.separator + ApplicationConstants.STDOUT);
        commands.add("2> " + ApplicationConstants.LOG_DIR_EXPANSION_VAR + File.separator + ApplicationConstants.STDERR);

        StringBuilder sb = new StringBuilder(200);
        for(String cmd: commands) {
            sb.append(cmd).append(" ");
        }
        return sb;
    }

    /**
     * Populate the environment string map to be added to the environment vars in a remote execution container.
     * 
     * @param env
     *            the map of env var values.
     * @param conf
     *            the Configuration to pull values from.
     */
    public static void addLocalClasspathToEnv(final Map<String, String> env, final Configuration conf) {
        StringBuilder classPathEnv = new StringBuilder(Environment.CLASSPATH.$());

        // add current folder
        classPathEnv.append(File.pathSeparatorChar).append("./*");

        // add yarn app classpath
        for(String cpEntry: conf.getStrings(YarnConfiguration.YARN_APPLICATION_CLASSPATH,
                YarnConfiguration.DEFAULT_YARN_APPLICATION_CLASSPATH)) {
            classPathEnv.append(File.pathSeparatorChar).append(cpEntry.trim());
        }

        for(String jar: conf.getStrings(MRJobConfig.MAPREDUCE_APPLICATION_CLASSPATH,
                org.apache.hadoop.util.StringUtils.getStrings(MRJobConfig.DEFAULT_MAPREDUCE_APPLICATION_CLASSPATH))) {
            classPathEnv.append(File.pathSeparatorChar).append(jar.trim());
        }

        // add the runtime classpath needed for tests to work
        if(conf.getBoolean(YarnConfiguration.IS_MINI_YARN_CLUSTER, false)) {
            classPathEnv.append(File.pathSeparatorChar).append(Environment.CLASSPATH.$());
        }

        // add guagua app jar file
        String path = getFileName(conf.get(GuaguaYarnConstants.GUAGUA_YARN_APP_JAR));
        classPathEnv.append(GuaguaYarnConstants.CURRENT_DIR).append(path).append(File.pathSeparatorChar);

        // Any libraries we may have copied over?
        String libs = conf.get(GuaguaYarnConstants.GUAGUA_YARN_APP_LIB_JAR);
        if(StringUtils.isNotEmpty(libs)) {
            for(String jar: Splitter.on(GuaguaYarnConstants.GUAGUA_APP_LIBS_SEPERATOR).split(libs)) {
                classPathEnv.append(GuaguaYarnConstants.CURRENT_DIR).append(getFileName(jar.trim()))
                        .append(File.pathSeparatorChar);
            }
        }

        // add log4j
        classPathEnv.append(GuaguaYarnConstants.CURRENT_DIR).append(GuaguaYarnConstants.GUAGUA_LOG4J_PROPERTIES)
                .append(File.pathSeparatorChar);

        // add guagua-conf.xml
        classPathEnv.append(GuaguaYarnConstants.CURRENT_DIR).append(GuaguaYarnConstants.GUAGUA_CONF_FILE);

        env.put(Environment.CLASSPATH.toString(), classPathEnv.toString());
    }

    private static String getFileName(String string) {
        return new Path(string).getName();
    }

    /**
     * Get path to store local resources on hdfs.
     */
    public static Path getPathForResource(FileSystem fs, String loc, ApplicationId appId) {
        return new Path(getAppDirectory(fs, appId), getFileName(loc));
    }

    /**
     * Working folder to store jars, files and other resources
     */
    public static Path getAppDirectory(FileSystem fs, ApplicationId appId) {
        return fs.makeQualified(new Path(
                new Path(File.separator + GUAGUA_YARN_TMP, GuaguaYarnConstants.GUAGUA_HDFS_DIR), appId.toString()));
    }

    /**
     * Copy batch of resources to hdfs app directory for master and containers.
     */
    public static void copyLocalResourcesToFs(Configuration conf, ApplicationId appId) throws IOException {
        FileSystem fs = FileSystem.get(conf);
        Path appDir = getAppDirectory(fs, appId);

        // copy yarn app jar
        String appJar = conf.get(GuaguaYarnConstants.GUAGUA_YARN_APP_JAR);
        copyToFs(conf, appJar, new Path(appDir, getFileName(appJar)).toString());

        // copy guagua-conf.xml: done one export method
        // copyToFs(conf, appJar, new Path(tempDir, GuaguaYarnConstants.GUAGUA_CONF_FILE).toString());

        // Copy all lib jars
        String libs = conf.get(GuaguaYarnConstants.GUAGUA_YARN_APP_LIB_JAR);
        if(StringUtils.isNotEmpty(libs)) {
            for(String jar: Splitter.on(GuaguaYarnConstants.GUAGUA_APP_LIBS_SEPERATOR).split(libs)) {
                copyToFs(conf, jar, new Path(appDir, getFileName(jar.trim())).toString());
            }
        }
    }

    private static void copyToFs(Configuration conf, String local, String remote) throws IOException {
        FileSystem fs = FileSystem.get(conf);
        Path src = new Path(local);
        Path dst = fs.makeQualified(new Path(remote));
        fs.copyFromLocalFile(false, true, src, dst);
    }

    /**
     * Boilerplate to add a file to the local resources..
     * 
     * @param localResources
     *            the LocalResources map to populate.
     * @param fs
     *            handle to the HDFS file system.
     * @param target
     *            the file to send to the remote container.
     */
    public static void addFileToResourceMap(Map<String, LocalResource> localResources, FileSystem fs, Path target)
            throws IOException {
        LocalResource resource = Records.newRecord(LocalResource.class);
        FileStatus destStatus = fs.getFileStatus(target);
        resource.setResource(ConverterUtils.getYarnUrlFromURI(target.toUri()));
        resource.setSize(destStatus.getLen());
        resource.setTimestamp(destStatus.getModificationTime());
        resource.setType(LocalResourceType.FILE); // use FILE, even for jars!
        resource.setVisibility(LocalResourceVisibility.APPLICATION);
        localResources.put(target.getName(), resource);
        LOG.info("Registered file in LocalResources :{} ", target);
    }

    /**
     * Copy local file to a named-file in remote FS
     */
    public static void copyLocalResourceToFs(String src, String dst, Configuration conf, ApplicationId appId)
            throws IOException {
        FileSystem fs = FileSystem.get(conf);
        Path tempDir = getAppDirectory(fs, appId);
        copyToFs(conf, src, new Path(tempDir, dst).toString());
    }

    /**
     * Export our populated Configuration as an XML file to be used by the ApplicationMaster's exec container, and
     * register it with LocalResources.
     * 
     * @param conf
     *            the current Configuration object to be published.
     * @param appId
     *            the ApplicationId to stamp this app's base HDFS resources dir.
     */
    public static void exportGuaguaConfiguration(Configuration conf, ApplicationId appId) throws IOException {
        FileSystem fs = FileSystem.get(conf);
        Path hdfsConfPath = new Path(getAppDirectory(fs, appId), GuaguaYarnConstants.GUAGUA_CONF_FILE);
        FSDataOutputStream fos = null;
        try {
            fos = FileSystem.get(conf).create(hdfsConfPath, true);
            conf.writeXml(fos);
            fos.flush();
        } finally {
            if(null != fos) {
                fos.close();
            }
        }
    }
}
