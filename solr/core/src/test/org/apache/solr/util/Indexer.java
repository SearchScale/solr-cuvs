package org.apache.solr.util;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.SolrRequest;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.request.GenericSolrRequest;
import org.apache.solr.client.solrj.request.RequestWriter;
import org.apache.solr.common.MapWriter;
import org.apache.solr.common.params.CommonParams;
import org.apache.solr.common.params.MapSolrParams;
import org.apache.solr.common.util.JavaBinCodec;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.Reader;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.LongAdder;

import static org.apache.solr.common.util.JavaBinCodec.*;

public class Indexer {
    static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();
    static JavaBinCodec.ObjectResolver FLOAT_ARR_RESOLVER = (o, c) -> {
        if (o instanceof float[]) {
            c.writeTag(ARR, ((float[]) o).length);
            for (float v : (float[]) o) {
                c.writeFloat(v);
            }
            return null;
        } else {
            return o;
        }
    };

    public static void indexDocs(SolrClient solrClient, long start, InputStream in, String coll, int batchSize) throws SolrServerException, IOException {
        BufferedReader br = new BufferedReader(new InputStreamReader(in));
        CSV csv = new CSV(br);
        LongAdder total = new LongAdder();

        System.out.println("\nStarting index %%%%%%%%%%%%%%%");

        for (; ; ) {
            String[] row = csv.readNext();
            if (row == null) break;
            indexBatch(solrClient, csv, row, batchSize, coll, total);
            System.out.println(total.sum());
        }
        long end = System.currentTimeMillis();
        System.out.println("\nTotal time: " + (double) (end - start) / 1000.0D);
    }

    public static void indexBatch(SolrClient solrClient, CSV csv, String[] firstRow, int batchSize, String coll, LongAdder total) throws SolrServerException, IOException {

        GenericSolrRequest gsr = new GenericSolrRequest(SolrRequest.METHOD.POST, "/update",
                new MapSolrParams(Map.of("commit", "true")))
                .setContentWriter(new RequestWriter.ContentWriter() {
                    @Override
                    public void write(OutputStream os) throws IOException {
                        JavaBinCodec codec = new JavaBinCodec(os, FLOAT_ARR_RESOLVER);

                        int errs = 0;
                        int counter = 0;

                        codec.writeTag(ITERATOR);
                        String[] row = firstRow;
                        for (; ; ) {
                            if (row == null) break;
                            long start = System.nanoTime();
                            try {
                                codec.writeMap(parse(row));
                                counter++;
                                System.out.print((System.nanoTime() - start) + " ");
                                if (counter % 100 == 0) System.out.println(counter);
                                if (counter >= batchSize) break;
                            } catch (IllegalArgumentException exp) {
                                errs++;
                            }
                            row = csv.readNext();
                        }
                        codec.writeTag(END);
                        total.add(counter);
                        codec.close();
                    }

                    @Override
                    public String getContentType() {
                        return CommonParams.JAVABIN_MIME;
                    }
                });

        gsr.process(solrClient, coll);
    }

  /*  static class Doc implements MapWriter {

        String id;
        String title;
        String article;
        float[] article_vector;
        boolean isErr = false;

        public Doc() {
        }


        public Doc(String[] row) {
            if (row.length < 4) {
                //invalid row
                isErr = true;
                return;
            }

        }

        @Override
        public void writeMap(EntryWriter ew) throws IOException {
            ew.put("id", id);
            ew.put("title", title);
            ew.put("article", article);
            ew.put("article_vector", article_vector);
        }
    }*/

    static MapWriter parse(String[] row) {
        String id;
        String title;
        String article;
        float[] article_vector;
        if (row.length < 4) {
            throw new IllegalArgumentException("Invalid row");
        }

        id = row[0];
        title = row[1];
        article = row[2];
        try {
            String json = row[3];
            if (json.charAt(0) != '[') {
                //Invalid row

                throw new IllegalArgumentException("Invalid json");
            }

            List<Float> floatList = OBJECT_MAPPER.readValue(json, valueTypeRef);
            article_vector = new float[floatList.size()];
            for (int i = 0; i < article_vector.length; i++) {
                article_vector[i] = floatList.get(i);
            }

            return ew -> {
                ew.put("id", id);
                ew.put("title", title);
                ew.put("article", article);
                ew.put("article_vector", article_vector);

            };


        } catch (Exception e) {
            throw new IllegalArgumentException("Invalid JSON");

        }

    }


    public static class CSV {
        String[] headers;
        final BufferedReader rdr;
        String line;
        boolean eof = false;


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
            return values.toArray(new String[0]);
        }

        public String[] readNext() throws IOException {
            if (eof) return null;
            line = this.rdr.readLine();
            if (line == null) {
                eof = true;
                return null;
            }
            String[] strings = parseLine(line);

            return strings;
        }
    }

    static TypeReference<List<Float>> valueTypeRef = new TypeReference<>() {
    };
}
