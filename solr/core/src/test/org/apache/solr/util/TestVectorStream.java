package org.apache.solr.util;

import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.SolrRequest;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.impl.CloudSolrClient;
import org.apache.solr.client.solrj.request.CollectionAdminRequest;
import org.apache.solr.client.solrj.request.GenericSolrRequest;
import org.apache.solr.client.solrj.request.JavaBinUpdateRequestCodec;
import org.apache.solr.client.solrj.request.RequestWriter;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.cloud.MiniSolrCloudCluster;
import org.apache.solr.cloud.SolrCloudTestCase;
import org.apache.solr.common.MapWriter;
import org.apache.solr.common.params.CommonParams;
import org.apache.solr.common.params.MapSolrParams;
import org.apache.solr.common.util.DataInputInputStream;
import org.apache.solr.common.util.JavaBinCodec;
import org.apache.solr.common.util.Utils;
import org.apache.solr.response.XMLResponseWriter;
import org.junit.Test;

import java.io.BufferedReader;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.Reader;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.zip.GZIPInputStream;

import static org.apache.solr.common.util.JavaBinCodec.ARR;
import static org.apache.solr.common.util.JavaBinCodec.ITERATOR;

public class TestVectorStream extends SolrCloudTestCase {

    @Test
    public void test() throws Exception {
        String testCollection = "test";

        MiniSolrCloudCluster cluster =
                configureCluster(1)
                        .addConfig(
                                "conf", TEST_PATH().resolve("configsets").resolve("cloud-managed").resolve("conf"))
                        .configure();
        try {
            System.setProperty("managed.schema.mutable", "true");
            CloudSolrClient client = cluster.getSolrClient();
            CollectionAdminRequest.createCollection(testCollection, "conf", 1, 1).process(client);
            modifySchema(testCollection, client);
            indexDocs(client,0, new GZIPInputStream(Files.newInputStream(TEST_PATH().resolve("wikipedia_vector_dump_100.csv.gz"))),
                    testCollection);
            QueryResponse resp = client.query(testCollection, new MapSolrParams(Map.of("q", "*:*")));

            System.out.println(resp.jsonStr());
        } finally {
            cluster.shutdown();

        }
    }

    public void testJavabin() throws Exception {

        List<Doc> l = new ArrayList<>();
        Doc d =  new Doc();
        d.id="1";
        d.title="T1";
        d.article="The article T1";
        d.article_vector=new float[]{1.0f,6.763487765f,9.67f};

        l.add(d);
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        JavaBinCodec codec = new JavaBinCodec(baos, null);
        codec.writeIterator(l.iterator());
        codec.close();

        Object result = new JavaBinCodec(){
            @Override
            public Object checkAndReadArray(DataInputInputStream dis) throws IOException {
                int sz = readSize(dis);
                tagByte =dis.readByte();
                if(tagByte == FLOAT){
                    float[] f = new float[sz];
                    f[0] = dis.readFloat();
                    for (int i = 1; i < sz; i++) {
                        tagByte = dis.readByte();
                        f[i] = dis.readFloat();
                    }
                    return f;
                } else {
                    ArrayList<Object> l = new ArrayList<>(sz);
                    l.add(readObject(dis));
                    for (int i = 1; i < sz; i++) {
                        l.add(readVal(dis));
                    }
                    return l;
                }

            }
        }
        .unmarshal(baos.toByteArray());
        System.out.println(Utils.toJSONString(result));
    }

    private void modifySchema(String testCollection, CloudSolrClient client)
            throws SolrServerException, IOException {
        GenericSolrRequest req =
                new GenericSolrRequest(SolrRequest.METHOD.POST, "/schema")
                        .setRequiresCollection(true)
                        .setContentWriter(
                                new RequestWriter.StringPayloadContentWriter(
                                        "{\n"
                                                + "\"add-field-type\" : {"
                                                + "\"name\":\"knn_vector\",\"class\":\"solr.DenseVectorField\",\"vectorDimension\":2048," +
                                                "\"similarityFunction\":\"cosine\",\"knnAlgorithm\":\"hnsw\"},\n"
                                                + "\"add-field\" : ["
                                                + "{\"name\":\"title\",\"type\":\"string\",\"stored\":true},\n"
                                                + "{\"name\":\"article\",\"type\":\"string\",\"stored\":true},\n"
                                                + "{\"name\":\"article_vector\",\"type\":\"knn_vector\",\"stored\":true},\n"+
                                        "]}",
                                        XMLResponseWriter.CONTENT_TYPE_XML_UTF8));

        client.request(req, testCollection);
    }

    private static void indexDocs(SolrClient solr, long start, InputStream in, String coll) throws SolrServerException, IOException {
        BufferedReader br = new BufferedReader(new InputStreamReader(in));

        CSV csv = new CSV(br);

        int counter = 0;
        GenericSolrRequest gsr = new GenericSolrRequest(SolrRequest.METHOD.POST, "/update",
                new MapSolrParams(Map.of("commit", "true")))
                .setContentWriter(new RequestWriter.ContentWriter() {
                    @Override
                    public void write(OutputStream os) throws IOException {
                        int counter = 0;
                        JavaBinCodec codec = new JavaBinCodec(os, null);
                        codec.writeTag(ITERATOR);
                        List<Doc> docs = new ArrayList<>();
                        for (;;) {
                            String[] row = csv.readNext();
                            if (row == null) break;
                            ++counter;
                            Doc d = new Doc(row);
                            if (d.isErr) continue;
                            docs.add(d);
                            codec.writeMap(d);
                        }
                        codec.close();
                        System.out.println("counter: "+ counter + " bytes: "+ codec.bytesWritten());
                    }
                    @Override
                    public String getContentType() {
                        return CommonParams.JAVABIN_MIME;
                    }
                });
        gsr.process(solr, "test");
        long end = System.currentTimeMillis();
        System.out.println("Total time: " + (double) (end - start) / 1000.0D);
    }

    static class Doc implements MapWriter {
        String id;
        String title;
        String article;
        float[] article_vector;
        boolean isErr = false;
        public Doc(){}


        public Doc(String[] row) {
            if (row.length < 4) {
                //invalid row
                isErr = true;
                return;
            }
            this.id = row[0];
            this.title = row[1];
            this.article = row[2];
            try {
                List<Object> vectorJson = (List<Object>) Utils.fromJSONString(row[3]);
                article_vector = new float[vectorJson.size()];
                for (int i = 0; i < vectorJson.size(); ++i) {
                    article_vector[i] = ((Number)vectorJson.get(i)).floatValue();
                }
            } catch (Exception e) {
                isErr = true;
            }
        }

        @Override
        public void writeMap(EntryWriter ew) throws IOException {
            ew.put("id", id);
            ew.put("title", title);
            ew.put("article", article);
            ew.put("article_vector", article_vector);
        }
    }

    public static class CSV {
        String[] headers;
        final BufferedReader rdr;
        String line;


        public CSV(Reader rdr) throws IOException {

            this.rdr = rdr instanceof BufferedReader ?
                    (BufferedReader) rdr :
                    new BufferedReader(rdr);
            String line = this.rdr.readLine();
            if (line == null)
                throw new RuntimeException("Empty or invalid CSV file.");
            headers = parseLine(line);
        }

        // Method to parse a single line of CSV, handling quoted fields
        private static String[] parseLine(String line) {
//         System.out.println(line);
            List<String> values = new ArrayList<>();
            StringBuilder currentValue = new StringBuilder();
            boolean inQuotes = false;
            char[] chars = line.toCharArray();

            for (int i = 0; i < chars.length; i++) {
                char currentChar = chars[i];

                if (currentChar == '"') {
                    // Toggle the inQuotes flag
                    inQuotes = !inQuotes;
                } else if (currentChar == ',' && !inQuotes) {
                    // If a comma is found and we're not inside quotes, end the current value
                    values.add(currentValue.toString());
                    currentValue = new StringBuilder();
                } else {
                    // Add the current character to the current value
                    currentValue.append(currentChar);
                }
            }

            // Add the last value
            values.add(currentValue.toString());
//         System.out.println("parsed : "+ values.toString());
            return values.toArray(new String[0]);
        }

        public String[] readNext() throws IOException {
            line = this.rdr.readLine();
            if (line == null) return null;
            return parseLine(line);
        }
    }
}
