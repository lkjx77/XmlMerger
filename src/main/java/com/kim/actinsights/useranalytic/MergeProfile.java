package com.kim.actinsights.useranalytic;
import com.kim.actsights.utils.StopWordFilter;
import com.kim.actsights.utils.Tokenizer;
import org.apache.crunch.*;
import org.apache.crunch.impl.mr.MRPipeline;
import org.apache.crunch.types.writable.Writables;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.crunch.CombineFn;

import java.io.File;
import java.net.URI;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class MergeProfile extends Configured implements Tool {

    public static void main(String[] args) throws Exception {
        ToolRunner.run(new Configuration(), new MergeProfile(), args);
    }

    static enum COUNTERS  {
        NO_MATCH,
        CORRUPT_SIZE
    }
    static final String logRegex = "^([\\d.]+) (\\S+) (\\S+) \\[([\\w:/]+\\s[+\\-]\\d{4})\\] \"(.+?)\" (\\d{3}) (\\d+) \"([^\"]+)\" \"([^\"]+)\"";


    public int run(String[] args) throws Exception {

        int rCode = 1;

        if (args.length != 2) {
            System.err.println("Usage: hadoop jar crunch-demo-1.0-SNAPSHOT-job.jar"
                    + " [generic options] input output");
            System.err.println();
            GenericOptionsParser.printGenericCommandUsage(System.err);
            return 1;
        }

        String inputPath = args[0];
        String outputPath = args[1];

        String hdfsUrl = "hdfs://localhost:9000";
        FileSystem fs = FileSystem.get(URI.create(hdfsUrl), getConf());
        Path output = new Path(outputPath);
        // true stands for recursively deleting the folder you gave
        if(fs.exists(output))
            fs.delete(output, true);

        // Create an object to coordinate pipeline creation and execution.
        Pipeline pipeline = new MRPipeline(MergeProfile.class, getConf());

        RemoteIterator<LocatedFileStatus> inputFiles = fs.listFiles(new Path(inputPath), true);
        while(inputFiles.hasNext())
        {
            System.out.println(inputFiles.next().getPath());
            // Reference a given text file as a collection of Strings.
            PCollection<String> lines = pipeline.readTextFile(inputFiles.next().getPath().toString());

            // Combiner used for summing up response size
            CombineFn<String, Long> longSumCombiner = CombineFn.SUM_LONGS();

            // Table of (ip, sum(response size))
            PTable<String, Long> ipAddrResponseSize =
                    lines.parallelDo(extractIPResponseSize,
                            Writables.tableOf(Writables.strings(),Writables.longs()))
                            .groupByKey()
                            .combineValues(longSumCombiner);

            pipeline.writeTextFile(ipAddrResponseSize, args[1]);

            // Execute the pipeline as a MapReduce.
            PipelineResult result = pipeline.done();

            System.out.println("status= " + result.status + " result = " + result.toString());
            rCode = result.succeeded() ? 0 : 1;
        }

        return rCode;
    }

    DoFn<String, Pair<String, Long>> extractIPResponseSize = new DoFn<String, Pair<String, Long>>() {
        transient Pattern pattern;
        public void initialize() {
            pattern = Pattern.compile(logRegex);
        }
        public void process(String line, Emitter<Pair<String, Long>> emitter) {
            Matcher matcher = pattern.matcher(line);
            if(matcher.matches()) {
                try {
                    Long requestSize = Long.parseLong(matcher.group(7));
                    String remoteAddr = matcher.group(1);
                    emitter.emit(Pair.of(remoteAddr, requestSize));
                } catch (NumberFormatException e) {
                    // corrupt line, we should increment counter
                }
            }
        }
    };
}


