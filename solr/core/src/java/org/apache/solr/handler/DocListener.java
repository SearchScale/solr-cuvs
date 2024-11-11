package org.apache.solr.handler;

import org.apache.lucene.document.Document;
import org.apache.lucene.index.IndexWriter;
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.common.annotation.JsonProperty;
import org.apache.solr.common.params.MapSolrParams;
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.common.util.ReflectMapWriter;
import org.apache.solr.core.SolrCore;
import org.apache.solr.request.LocalSolrQueryRequest;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.schema.IndexSchema;
import org.apache.solr.update.CommitUpdateCommand;
import org.apache.solr.update.DocumentBuilder;
import org.apache.solr.util.RefCounted;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.lang.invoke.MethodHandles;
import java.util.Map;

public class DocListener implements JavabinBulkReader.Listener, ReflectMapWriter {
    private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

    private final IndexSchema schema;
    RefCounted<IndexWriter> iw;
    IndexWriter w;

    @JsonProperty
    public final boolean commit;
    // only for perf testing purpose
    @JsonProperty
    public final boolean discard;
    long startTime;

    private final SolrCore core;




    @JsonProperty
    public int totalDocs;


    DocListener( SolrParams p, SolrCore core) {
        this.schema = core.getLatestSchema();
        commit = p.getBool("commit", false);
        discard = p.getBool("discard", false);
        this.core = core;
    }

    @Override
    public void start() throws IOException {
        startTime = System.currentTimeMillis();
        log.info("START_STREAM");
        iw = core.getSolrCoreState().getIndexWriter(core);
        w = iw.get();
    }

    @Override
    public void doc(SolrInputDocument sid) throws IOException {
        totalDocs++;
//          if(counter%100 ==0) System.out.println();
        Document d = DocumentBuilder.toDocument(sid, schema, false, true);
//          System.out.println(d.getFields().size() +" "+d.get("id"));
        if (!discard) {
            w.addDocument(d);
        }

    }

    @JsonProperty
    public long indexTime;
    @JsonProperty
    public long commitTime;

    @Override
    public void end() throws IOException {
        indexTime = System.currentTimeMillis() - startTime;
        log.info("END_STREAM: docs:{}, time taken : {} ", +totalDocs, indexTime);
        iw.decref();
        if (commit) {
            long b4Commit = System.currentTimeMillis();
            log.info("gonna commit");
            MapSolrParams args = new MapSolrParams(Map.of("commit", "true"));
            SolrQueryRequest req = new LocalSolrQueryRequest(core, args);
            core.getUpdateHandler().commit(new CommitUpdateCommand(req, false));
            commitTime = System.currentTimeMillis() - b4Commit;
            log.info("done commit: {}", commitTime);
        }
    }
}
