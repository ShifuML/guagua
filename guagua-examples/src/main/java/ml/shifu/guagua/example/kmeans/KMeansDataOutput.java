/*
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
package ml.shifu.guagua.example.kmeans;

import java.io.IOException;
import java.io.PrintWriter;
import java.util.List;

import ml.shifu.guagua.worker.BasicWorkerInterceptor;
import ml.shifu.guagua.worker.WorkerContext;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * {@link KMeansDataOutput} is used to save tagged data into HDFS.
 */
public class KMeansDataOutput extends BasicWorkerInterceptor<KMeansMasterParams, KMeansWorkerParams> {

    private static final Logger LOG = LoggerFactory.getLogger(KMeansDataOutput.class);

    @Override
    public void postApplication(WorkerContext<KMeansMasterParams, KMeansWorkerParams> context) {
        Path outFolder = new Path(context.getProps().getProperty(KMeansContants.KMEANS_DATA_OUTPUT,
                "part-g-" + context.getContainerId()));
        String separator = context.getProps().getProperty(KMeansContants.KMEANS_DATA_SEPERATOR);

        @SuppressWarnings("unchecked")
        List<TaggedRecord> dataList = (List<TaggedRecord>) context.getAttachment();

        PrintWriter pw = null;
        try {
            FileSystem fileSystem = FileSystem.get(new Configuration());
            fileSystem.mkdirs(outFolder);

            Path outputFile = new Path(outFolder, "part-g-" + context.getContainerId());
            FSDataOutputStream fos = fileSystem.create(outputFile);
            LOG.info("Writing results to {}", outputFile.toString());
            pw = new PrintWriter(fos);
            for(TaggedRecord record: dataList) {
                pw.println(record.toString(separator));
            }
            pw.flush();
        } catch (IOException e) {
            LOG.error("Error in writing output.", e);
        } finally {
            IOUtils.closeStream(pw);
        }
    }

}
