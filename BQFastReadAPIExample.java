package bq;

/*
Copyright 2023 Google LLC

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    https://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.

by Keys Botzum
*/

/**
 * A simple exmaple of using the BQ read API to efficiently read a large query response. In this case, the code is reading from a readily available
 * public dataset - information about bitcoin trades.
 * 
 * The key to high throughput is using the read API along with multiple threads to read from multiple BQ streams. To ensure that we really
 * get the needed parallelism, we must ensure that the BQ client allocates multiple threads for the underlying gRPC threads. This isn't 
 * done by default.
 * 
 * This example includes a few compile time settings to help you understand different approaches. They are:
 *     - Compression on or off
 *     - Whether to use the read API directly against the table (where clause only) or to use a full SQL query job, followed by the read API
 *       Using the read API directly is more efficient *if* a simple where clause is viable
 *     - The max number of parallel streams to use
 * 
 * This is based upon the public Google BQ example document with substantial enhancements for throughput. See the base documentation and
 * original sample here: https://cloud.google.com/bigquery/docs/reference/storage, https://cloud.google.com/bigquery/docs/reference/storage/libraries.
 * 
 */
import java.io.IOException;
import java.math.BigDecimal;
import java.text.NumberFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import org.apache.arrow.compression.CommonsCompressionFactory;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.ValueVector;
import org.apache.arrow.vector.VectorLoader;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.ipc.ReadChannel;
import org.apache.arrow.vector.ipc.message.MessageSerializer;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.arrow.vector.util.ByteArrayReadableSeekableByteChannel;
import org.apache.arrow.vector.util.Text;

import com.google.api.gax.grpc.ChannelPoolSettings;
import com.google.api.gax.grpc.InstantiatingGrpcChannelProvider;
import com.google.api.gax.rpc.ServerStream;
import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.BigQueryOptions;
import com.google.cloud.bigquery.Job;
import com.google.cloud.bigquery.JobInfo;
import com.google.cloud.bigquery.QueryJobConfiguration;
import com.google.cloud.bigquery.TableId;
import com.google.cloud.bigquery.storage.v1.ArrowRecordBatch;
import com.google.cloud.bigquery.storage.v1.ArrowSchema;
import com.google.cloud.bigquery.storage.v1.ArrowSerializationOptions;
import com.google.cloud.bigquery.storage.v1.BigQueryReadClient;
import com.google.cloud.bigquery.storage.v1.BigQueryReadSettings;
import com.google.cloud.bigquery.storage.v1.CreateReadSessionRequest;
import com.google.cloud.bigquery.storage.v1.DataFormat;
import com.google.cloud.bigquery.storage.v1.ReadRowsRequest;
import com.google.cloud.bigquery.storage.v1.ReadRowsResponse;
import com.google.cloud.bigquery.storage.v1.ReadSession;
import com.google.cloud.bigquery.storage.v1.ReadSession.TableReadOptions;
import com.google.common.base.Preconditions;

public class BQFastReadAPIExample {
    // source data set information
    static String projectName = "bigquery-public-data";
    static String datasetName = "crypto_bitcoin";
    static String tableName = "transactions";

    /*
     * query, then read. If false, just rely on read filters which are more limited
     * but much faster for large results since you avoid repartition steps that
     * create the temporary table, Of course this requires that the query be simple
     * enough to execute entirely as a where clause essentially - no DISTINCT, no
     * JOIN, etc.
     */
    static boolean queryFirst = true;

    /*
     * maximum streams to tell BQ to provide. In general, changing this makes a big
     * difference and more so if your client has more networking capacity.
     * Eventually the client bottlenecks on the network if maxStreams is high
     * enough.
     */
    static int maxStreams = 25;

    /*
     * use zstd compression to reduce message size. Looks like almost 60% for my
     * tests.
     */
    static boolean useCompression = true;

    /*
     * display samples of the response data. Will display first row of each stream
     * from BQ.
     */

    static boolean displaySamples = true;

    static String whereClause = "block_timestamp_month > '2022-01-01' AND block_timestamp_month < '2023-01-01' AND size in (2324,128,72,2000, 254, 225, 278, 2372, 1872, 163,"
            + "164, 165, 170, 175, 250, 251, 252, 253, 545, 546, 547, 548, 549, 550, 551, 552, 553, 610, 611, 612, 613, 614, 615, 616, 830, 831, 832, 833, 834,"
            + "835, 836, 837, 838, 839, 840, 841, 842, 843, 844, 845, 846, 847, 848, 849, 850, 851, 852, 853, 854, 855, 856, 857, 858, 859, 860, 861, 862, 863)";

    /**
     * Inner class to handle the processing of the received Arrow buffers from each
     * BQ stream.
     */
    private static class SimpleRowReader implements AutoCloseable, Runnable {
        BufferAllocator allocator = new RootAllocator(Long.MAX_VALUE);

        // The root decoder object will be reused to avoid re-allocation and
        // too much garbage collection.
        private VectorSchemaRoot root;
        private VectorLoader loader;
        private ServerStream<ReadRowsResponse> stream;
        int numEntries = 0;
        long rawMessageBytes = 0;

        public SimpleRowReader(ArrowSchema arrowSchema, ReadRowsRequest readRowsRequest, BigQueryReadClient client)
                throws IOException {

            this.stream = client.readRowsCallable().call(readRowsRequest);

            // extract the arrow schema from the arrow buffer so that I can initialize the
            // root
            Schema schema = MessageSerializer.deserializeSchema(
                    new ReadChannel(
                            new ByteArrayReadableSeekableByteChannel(
                                    arrowSchema.getSerializedSchema().toByteArray())));

            Preconditions.checkNotNull(schema);

            // create a VectorSchemaRoot from the schema. the root can then be used to
            // process the content of this stream of buffers.
            root = VectorSchemaRoot.create(schema, allocator);

            if (useCompression) {
                loader = new VectorLoader(root, CommonsCompressionFactory.INSTANCE);

            } else {
                loader = new VectorLoader(root);
            }
        }

        /**
         * Read a single BQ stream of arrow buffers. This leverages the previously
         * created Arrow root object to properly interprete the bytes
         * via the schema that was specified then.
         */
        public void run() {
            boolean haveDisplayedFirst = false;

            try {
                for (ReadRowsResponse response : stream) {
                    rawMessageBytes += response.getSerializedSize();

                    Preconditions.checkState(response.hasArrowRecordBatch());
                    ArrowRecordBatch batch = response.getArrowRecordBatch();

                    org.apache.arrow.vector.ipc.message.ArrowRecordBatch deserializedBatch = MessageSerializer
                            .deserializeRecordBatch(
                                    new ReadChannel(
                                            new ByteArrayReadableSeekableByteChannel(
                                                    batch.getSerializedRecordBatch().toByteArray())),
                                    allocator);

                    loader.load(deserializedBatch);
                    // Release buffers from batch, they are still held by root
                    deserializedBatch.close();

                    if (displaySamples && !haveDisplayedFirst) {
                        haveDisplayedFirst = true;
                        // prints entire vector of rows from this batch, not just first row
                        // System.out.print("raw print: " + root.contentToTSVString());

                        /*
                         * print each row in this batch
                         * get the vector readers for `hash`, size, , block_timestamp_month fee "
                         * type mapping information is found in these places:
                         * https://cloud.google.com/bigquery/docs/reference/storage/
                         * https://arrow.apache.org/docs/java/reference/org/apache/arrow/vector/complex/
                         * reader/FieldReader.html
                         * https://arrow.apache.org/docs/java/reference/org/apache/arrow/vector/
                         * ValueVector.html
                         */
                        ValueVector hash_v = root.getVector("hash");
                        ValueVector size_v = root.getVector("size");
                        ValueVector month_v = root.getVector("block_timestamp_month");
                        ValueVector fee_v = root.getVector("fee");

                        String hash = ((Text) hash_v.getObject(0)).toString();
                        long size = ((Long) size_v.getObject(0)).longValue();
                        int date = ((Integer) month_v.getObject(0)).intValue();
                        BigDecimal fee = ((BigDecimal) fee_v.getObject(0));

                        System.out.println(hash + "\t" + size + "\t" + date + "\t" + fee);
                    }

                    numEntries += root.getRowCount();

                    root.clear();
                }
            } catch (Exception e) {
                System.out.println("In run() " + e);
                e.printStackTrace();
            }
        }

        @Override
        public void close() {
            root.close();
            allocator.close();
        }
    }

    public static void main(String... args) throws Exception {
        System.out.println("start: " + new Date());
        long startTime = new Date().getTime();
        String projectId = args[0];

        // Form table name for dataset
        String baseTable = String.format(
                "projects/%s/datasets/%s/tables/%s", projectName, datasetName, tableName);

        String srcTable;
        double queryTimeSeconds = 0;

        if (queryFirst) {
            System.out.println("Starting query job");
            String query = "select ";
            String fromTable = String.format("%s.%s.%s", projectName, datasetName, tableName);

            query += "`hash`, size, fee, block_timestamp_month from "
                    + "`" + fromTable + "`"
                    + " where " + whereClause;

            System.out.println("Starting query job with query " + query);

            BigQuery bigquery = BigQueryOptions.newBuilder().setProjectId(projectId).build().getService();
            QueryJobConfiguration queryConfig = QueryJobConfiguration.newBuilder(query).build();
            Job job = bigquery.create(JobInfo.of(queryConfig));
            job.waitFor();
            queryTimeSeconds = ((new Date()).getTime() - startTime) / 1000.;
            System.out.println("query job finished in " + queryTimeSeconds + " seconds");

            // get temp table id and convert into a proper table string for using the read
            // API
            TableId tableId = ((QueryJobConfiguration) job.getConfiguration()).getDestinationTable();

            srcTable = String.format("projects/%s/datasets/%s/tables/%s",
                    tableId.getProject(), tableId.getDataset(), tableId.getTable());
        } else {
            // for reading directly from source table without any preceeding query
            srcTable = baseTable;
        }

        BigQueryReadSettings clientSettings;
        /*
         * Increase threads on the client itself since it will be shared by many threads
         * reading from the sessions in parallel. if we don't do this, we end up with
         * one single gRPC thread and bottleneck when the client is shared.
         * 
         * This is tricky as the client is short lived and thus the auto-scaling of the
         * channel that occurs once per minute is of little value. Instead I just set
         * the minimum number of channels (each of which can process one RPC at a time)
         * to be proportional to the number of sessions - because that is
         * proportional to the number of threads and those threads are very busy.
         * 
         * In my very limited testing, it appears that one gRPC channel per
         * thread/session is optimal for this use case - CPU bound reading of response
         * stream.
         * 
         * If this client ran for longer, gRPC should shrink the pool if it is
         * unnecessarily large although I did not test that.
         */

        clientSettings = BigQueryReadSettings.newBuilder()
                .setTransportChannelProvider(
                        InstantiatingGrpcChannelProvider.newBuilder()
                                .setChannelPoolSettings(ChannelPoolSettings.builder()
                                        .setInitialChannelCount(maxStreams).build())
                                .build())
                .build();

        BigQueryReadClient client = BigQueryReadClient.create(clientSettings);
        String parent = String.format("projects/%s", projectId);

        // We specify the columns to be projected by adding them to the selected fields,
        // and set a simple filter to restrict which rows are transmitted.
        TableReadOptions.Builder build = TableReadOptions.newBuilder()
                .addSelectedFields("hash")
                .addSelectedFields("fee")
                .addSelectedFields("block_timestamp_month")
                .addSelectedFields("size");

        if (!queryFirst) {
            // filter here since didn't filter in the query
            build.setRowRestriction(whereClause);
        }

        if (useCompression) {
            // LZ4 doesn't result in data size reductions as good as ZSTD so use ZSTD
            // In my test LZ4 reduced bytes by about 25% while ZSTD hit nearly 60%
            ArrowSerializationOptions arrowoptions = ArrowSerializationOptions.newBuilder()
                    .setBufferCompression(ArrowSerializationOptions.CompressionCodec.ZSTD).build();
            build.setArrowSerializationOptions(arrowoptions);
        }
        TableReadOptions options = build.build();

        // Start specifying the read session we want created.
        ReadSession.Builder sessionBuilder = ReadSession.newBuilder()
                .setTable(srcTable)
                // This API can also deliver data serialized in Apache Avro format.
                // This example leverages Apache Arrow.
                .setDataFormat(DataFormat.ARROW)
                .setReadOptions(options);

        // Begin building the session creation request.
        CreateReadSessionRequest.Builder builder = CreateReadSessionRequest.newBuilder()
                .setParent(parent)
                .setReadSession(sessionBuilder)
                .setMaxStreamCount(maxStreams);

        ReadSession session = client.createReadSession(builder.build());
        int streamCount = session.getStreamsCount();
        System.out.println("Stream count: " + streamCount);

        /*
         * Create a thread pool with one thread per session to read in parallel.
         */
        ExecutorService es;
        es = Executors.newCachedThreadPool();

        System.out.println("Starting worker threads");
        List<SimpleRowReader> readers = new ArrayList<>();
        for (int i = 0; i < streamCount; i++) {
            // Use the stream to perform reading.
            String streamName = session.getStreams(i).getName();
            ReadRowsRequest readRowsRequest = ReadRowsRequest.newBuilder().setReadStream(streamName).build();

            SimpleRowReader reader;
            reader = new SimpleRowReader(session.getArrowSchema(), readRowsRequest, client);

            readers.add(reader);
            es.execute(reader);
        }
        es.shutdown();

        System.out.println("\n\nStarting shutdwn. Reminder, total streams: " + streamCount);
        boolean finished = false;
        int maxIterations = 20; // 10 minutes
        while (!finished) {
            // wait 30 seconds
            finished = es.awaitTermination(30, TimeUnit.SECONDS);

            if (finished) {
                break;
            }

            // wait failed, so check situation
            if (es instanceof ThreadPoolExecutor) {
                System.out.println(
                        "Pool size is now " +
                                ((ThreadPoolExecutor) es).getActiveCount());
            }

            maxIterations--;
            if (maxIterations <= 0) {
                break;
            }
        }
        if (finished)
            System.out.println("\n\nTerminated all threads successfully");

        int numRows = 0;
        long numBytes = 0;
        for (int i = 0; i < readers.size(); i++) {
            SimpleRowReader reader = readers.get(i);
            numRows += reader.numEntries;
            numBytes += reader.rawMessageBytes;
            reader.close();
        }

        /*
         * All done, print out various bits of information.
         */

        NumberFormat myFormat = NumberFormat.getInstance();
        myFormat.setGroupingUsed(true);

        double totalTime = ((new Date()).getTime() - startTime) / 1000.;
        if (queryFirst) {
            System.out.println("Query time seconds: " + queryTimeSeconds);
            System.out.println("Read time seconds: " + myFormat.format(totalTime - queryTimeSeconds));
        }
        System.out.println("Total time seconds: " + totalTime);
        System.out.println("Compression state is " + useCompression);

        System.out.println("Total rows: " + myFormat.format(numRows) + ", total bytes: " + myFormat.format(numBytes)
                + ", total streams: " + streamCount);

        System.out.println("Mode information: ");
        System.out.println("  Query first: " + queryFirst);

        System.out.println("  Max streams: " + maxStreams);

        System.out.println(new Date());
    }

}
