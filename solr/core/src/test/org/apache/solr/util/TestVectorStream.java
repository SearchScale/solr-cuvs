package org.apache.solr.util;

import org.apache.solr.client.solrj.SolrRequest;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.impl.CloudSolrClient;
import org.apache.solr.client.solrj.request.CollectionAdminRequest;
import org.apache.solr.client.solrj.request.GenericSolrRequest;
import org.apache.solr.client.solrj.request.RequestWriter;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.cloud.MiniSolrCloudCluster;
import org.apache.solr.cloud.SolrCloudTestCase;
import org.apache.solr.common.MapWriter;
import org.apache.solr.common.params.MapSolrParams;
import org.apache.solr.common.util.DataInputInputStream;
import org.apache.solr.common.util.JavaBinCodec;
import org.apache.solr.response.XMLResponseWriter;
import org.junit.Test;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.zip.GZIPInputStream;


public class TestVectorStream extends SolrCloudTestCase {

    @Test
    public void test() throws Exception {
        String testCollection = "test";

        MiniSolrCloudCluster cluster =
                configureCluster(1)
                        .addConfig(
                                "conf", TEST_PATH().resolve("configsets")
                                        .resolve("cloud-managed")
                                        .resolve("conf"))
                        .configure();
        try {
            System.setProperty("managed.schema.mutable", "true");
            CloudSolrClient client = cluster.getSolrClient();
            CollectionAdminRequest.createCollection(testCollection, "conf", 1, 1).process(client);
            modifySchema(testCollection, client);
            modifyConfig(testCollection, client);
            //wikipedia_vector_dump_100.csv.gz
            //10k_wiki.csv.gz
            try(GZIPInputStream in = new GZIPInputStream(Files.newInputStream(TEST_PATH().resolve("10k_wiki.csv.gz")))) {
                Indexer.indexDocs(client, 0, in,
                        testCollection, 100000, 1);
            }
            QueryResponse resp = client.query(testCollection, new MapSolrParams(Map.of("q", "*:*")));
            assertEquals(100,resp.getResults().getNumFound());
            System.out.println("num docs: "+ resp.getResults().getNumFound());
        } finally {
            cluster.shutdown();

        }
    }

    public void testJavabin() throws Exception {

        List<MapWriter> l = new ArrayList<>();


        l.add(Indexer.parse(new String[]{"1","T1", "The article T1", "{1.0f,6.763487765f,9.67f, 985.37855f}"}));
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        JavaBinCodec codec = new JavaBinCodec(baos, null);
        codec.writeIterator(l.iterator());
        codec.close();

        Object result = new JavaBinCodec(){
            @Override
            public Object checkAndReadArray(DataInputInputStream dis) throws IOException {
                int sz = readSize(dis);
                tagByte =dis.readByte();
                if(tagByte == FLOAT) {
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
        List<Object> list = (List<Object>) result;

        Object article_vector = ((Map) list.get(0)).get("article_vector");
        assertTrue(article_vector instanceof float[]);
        assertEquals( ((float[])article_vector).length , 4);

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
    private void modifyConfig(String testCollection, CloudSolrClient client)
            throws SolrServerException, IOException {
        GenericSolrRequest req =
                new GenericSolrRequest(SolrRequest.METHOD.POST, "/config")
                        .setRequiresCollection(true)
                        .setContentWriter(
                                new RequestWriter.StringPayloadContentWriter(
                                        "{\n" +
                                                "           \"add-requesthandler\": {\n" +
                                                "             \"name\": \"/directupdate\",\n" +
                                                "             \"class\": \"solr.DirectIndexingRequestHandler\"\n" +
                                                "             }\n" +
                                                "         }",
                                        XMLResponseWriter.CONTENT_TYPE_XML_UTF8));

        client.request(req, testCollection);
    }


}
