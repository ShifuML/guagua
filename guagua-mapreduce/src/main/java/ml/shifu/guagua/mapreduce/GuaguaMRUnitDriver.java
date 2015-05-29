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
package ml.shifu.guagua.mapreduce;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import ml.shifu.guagua.io.Bytable;
import ml.shifu.guagua.io.GuaguaFileSplit;
import ml.shifu.guagua.unit.GuaguaUnitDriver;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionCodecFactory;
import org.apache.hadoop.util.StringUtils;

/**
 * {@link GuaguaMRUnitDriver} is used to run in-memory guagua application by using hadoop MapReduce splits.
 * 
 * @param <MASTER_RESULT>
 *            master result for computation in each iteration.
 * @param <WORKER_RESULT>
 *            worker result for computation in each iteration.
 * 
 * @see ml.shifu.guagua.mapreduce.example.sum.SumTest in guagua-mapreduce-examples project.
 * 
 * @deprecated use {@link ml.shifu.guagua.hadoop.io.GuaguaMRUnitDriver}
 */
@Deprecated
public class GuaguaMRUnitDriver<MASTER_RESULT extends Bytable, WORKER_RESULT extends Bytable> extends
        GuaguaUnitDriver<MASTER_RESULT, WORKER_RESULT> {

    /**
     * A only constructor here for local in-memory guagua job.
     * 
     * @param props
     *            set all the configurations like input, output and ..
     * 
     * @see ml.shifu.guagua.mapreduce.example.sum.SumTest in guagua-mapreduce-examples project.
     * 
     */
    public GuaguaMRUnitDriver(Properties props) {
        super(props);
    }

    /**
     * Whether it is not pig or hadoop meta output file.
     */
    private boolean isPigOrHadoopMetaFile(Path path) {
        return path.toString().indexOf(GuaguaMapReduceConstants.HADOOP_SUCCESS) >= 0
                || path.toString().indexOf(GuaguaMapReduceConstants.PIG_HEADER) >= 0
                || path.toString().indexOf(GuaguaMapReduceConstants.PIG_SCHEMA) >= 0;
    }

    /**
     * Check whether file is splitable in HDFS.
     */
    private boolean isSplitable(Configuration conf, Path file) {
        // bzip2 can be split.
        if(file.getName().endsWith(GuaguaMapReduceConstants.BZ2)) {
            return true;
        }
        // other compression can not be split, maybe for lzo I should add it to split list.
        CompressionCodec codec = new CompressionCodecFactory(conf).getCodec(file);
        return codec == null;
    }

    @Override
    public List<GuaguaFileSplit[]> generateWorkerSplits(String inputs) throws IOException {
        List<GuaguaFileSplit[]> splits = new ArrayList<GuaguaFileSplit[]>();
        Configuration conf = new Configuration();
        // generate splits
        List<FileStatus> files = listStatus(conf, inputs);
        for(FileStatus file: files) {
            Path path = file.getPath();
            if(isPigOrHadoopMetaFile(path)) {
                continue;
            }
            long length = file.getLen();
            if((length != 0) && isSplitable(conf, path)) {
                long splitSize = file.getBlockSize();

                long bytesRemaining = length;
                while(((double) bytesRemaining) / splitSize > GuaguaMapReduceConstants.SPLIT_SLOP) {
                    splits.add(new GuaguaFileSplit[] { new GuaguaFileSplit(path.toString(), length - bytesRemaining,
                            splitSize) });
                    bytesRemaining -= splitSize;
                }

                if(bytesRemaining != 0) {
                    splits.add(new GuaguaFileSplit[] { new GuaguaFileSplit(path.toString(), length - bytesRemaining,
                            bytesRemaining) });
                }
            } else if(length != 0) {
                splits.add(new GuaguaFileSplit[] { new GuaguaFileSplit(path.toString(), 0, length) });
            }
        }
        return splits;
    }

    /**
     * Get the list of input {@link Path}s for the map-reduce job.
     */
    private static Path[] getInputPaths(String inputs) {
        String[] list = StringUtils.split(inputs);
        Path[] result = new Path[list.length];
        for(int i = 0; i < list.length; i++) {
            result[i] = new Path(StringUtils.unEscapeString(list[i]));
        }
        return result;
    }

    private static final PathFilter hiddenFileFilter = new PathFilter() {
        public boolean accept(Path p) {
            String name = p.getName();
            return !name.startsWith("_") && !name.startsWith(".");
        }
    };

    /**
     * Proxy PathFilter that accepts a path only if all filters given in the constructor do. Used by the listPaths() to
     * apply the built-in hiddenFileFilter together with a user provided one (if any).
     */
    private static class MultiPathFilter implements PathFilter {
        private List<PathFilter> filters;

        public MultiPathFilter(List<PathFilter> filters) {
            this.filters = filters;
        }

        public boolean accept(Path path) {
            for(PathFilter filter: filters) {
                if(!filter.accept(path)) {
                    return false;
                }
            }
            return true;
        }
    }

    /**
     * List input directories.
     * Subclasses may override to, e.g., select only files matching a regular expression.
     * 
     * @param job
     *            the job to list input paths for
     * @return array of FileStatus objects
     * @throws IOException
     *             if zero items or any IOException for input files.
     */
    protected List<FileStatus> listStatus(Configuration conf, String input) throws IOException {
        List<FileStatus> result = new ArrayList<FileStatus>();
        Path[] dirs = getInputPaths(input);
        if(dirs.length == 0) {
            throw new IOException("No input paths specified in job");
        }

        List<IOException> errors = new ArrayList<IOException>();

        // creates a MultiPathFilter with the hiddenFileFilter and the
        // user provided one (if any).
        List<PathFilter> filters = new ArrayList<PathFilter>();
        filters.add(hiddenFileFilter);
        PathFilter inputFilter = new MultiPathFilter(filters);

        for(int i = 0; i < dirs.length; ++i) {
            Path p = dirs[i];
            FileSystem fs = p.getFileSystem(conf);
            FileStatus[] matches = fs.globStatus(p, inputFilter);
            if(matches == null) {
                errors.add(new IOException("Input path does not exist: " + p));
            } else if(matches.length == 0) {
                errors.add(new IOException("Input Pattern " + p + " matches 0 files"));
            } else {
                for(FileStatus globStat: matches) {
                    if(globStat.isDir()) {
                        for(FileStatus stat: fs.listStatus(globStat.getPath(), inputFilter)) {
                            result.add(stat);
                        }
                    } else {
                        result.add(globStat);
                    }
                }
            }
        }

        if(!errors.isEmpty()) {
            throw new IOException(errors.toString());
        }
        return result;
    }

}
