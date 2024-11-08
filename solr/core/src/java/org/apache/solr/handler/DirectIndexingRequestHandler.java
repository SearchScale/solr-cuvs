/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.solr.handler;

import org.apache.lucene.document.Document;
import org.apache.lucene.index.IndexWriter;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.common.SolrInputField;
import org.apache.solr.common.annotation.JsonProperty;
import org.apache.solr.common.params.MapSolrParams;
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.common.util.ContentStream;
import org.apache.solr.common.util.DataInputInputStream;
import org.apache.solr.common.util.JavaBinCodec;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.common.util.ReflectMapWriter;
import org.apache.solr.core.SolrCore;
import org.apache.solr.request.LocalSolrQueryRequest;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.response.SolrQueryResponse;
import org.apache.solr.schema.IndexSchema;
import org.apache.solr.security.AuthorizationContext;
import org.apache.solr.update.CommitUpdateCommand;
import org.apache.solr.update.DocumentBuilder;
import org.apache.solr.util.RefCounted;
import org.apache.solr.util.plugin.SolrCoreAware;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.lang.invoke.MethodHandles;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

public class DirectIndexingRequestHandler extends RequestHandlerBase implements SolrCoreAware {
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  private SolrCore core;

  @Override
  public void init(NamedList<?> args) {
    super.init(args);
  }

  @Override
  public void inform(SolrCore core) {
    this.core = core;
  }
  public class Listener implements MapIterReader.Listener, ReflectMapWriter {
      private final IndexSchema schema;
      RefCounted<IndexWriter> iw ;
      IndexWriter w;

      @JsonProperty
      public final boolean commit;
      // only for perf testing purpose
      @JsonProperty
      public final boolean discard;
      long startTime;


      @JsonProperty
      public int totalDocs;


      Listener(SolrParams p) {
          this.schema = core.getLatestSchema();
          commit = p.getBool("commit", false);
          discard = p.getBool("discard", false);
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
          if(!discard){
              w.addDocument(d);
          }

      }
      @JsonProperty
      public long indexTime;
      @JsonProperty
      public long commitTime;

      @Override
      public void end() throws IOException {
          indexTime =  System.currentTimeMillis() - startTime;
          log.info("END_STREAM: docs:{}, time taken : {} ", +totalDocs,indexTime);
          iw.decref();
          if(commit) {
              long b4Commit = System.currentTimeMillis();
              log.info("gonna commit");
              MapSolrParams args = new MapSolrParams(Map.of("commit", "true"));
              SolrQueryRequest req = new LocalSolrQueryRequest(core, args);
              core.getUpdateHandler().commit(new CommitUpdateCommand(req, false));
              commitTime = System.currentTimeMillis() - b4Commit;
              log.info("done commit: {}",commitTime);
          }
      }
  }

  @Override
  public void handleRequestBody(SolrQueryRequest req, SolrQueryResponse rsp) throws Exception {
      ContentStream is = extractSingleContentStream(req);
      Listener listener = new Listener(req.getParams());
      new MapIterReader(listener).unmarshal(is.getStream());
      rsp.getValues().add("stats", listener);
  }

  private ContentStream extractSingleContentStream(SolrQueryRequest req) {
    Iterable<ContentStream> streams = req.getContentStreams();
    String exceptionMsg =
            "DocumentAnalysisRequestHandler expects a single content stream with documents to analyze";
    if (streams == null) {
      throw new SolrException(SolrException.ErrorCode.BAD_REQUEST, exceptionMsg);
    }
    Iterator<ContentStream> iter = streams.iterator();
    if (!iter.hasNext()) {
      throw new SolrException(SolrException.ErrorCode.BAD_REQUEST, exceptionMsg);
    }
    ContentStream stream = iter.next();
    if (iter.hasNext()) {
      throw new SolrException(SolrException.ErrorCode.BAD_REQUEST, exceptionMsg);
    }
    return stream;
  }

  @Override
  public String getDescription() {
    return "Reports application health to a load-balancer";
  }

  @Override
  public Boolean registerV2() {
    return Boolean.TRUE;
  }

  @Override
  public Category getCategory() {
    return Category.ADMIN;
  }

  @Override
  public Name getPermissionName(AuthorizationContext request) {
    return null;
  }
  public static class MapIterReader extends JavaBinCodec {
      final Listener docSink;

      private NamedList<Object> nl = new NamedList<>(10) {
          @Override
          public void add(String name, Object val) {
              SolrInputField f = new SolrInputField(name);
              f.setValue(val);
              super.add(name, f);
          }
      };
      @SuppressWarnings("rawtypes")
      final Map reusedMap = nl.asShallowMap(true);

      @SuppressWarnings("unchecked")
      final SolrInputDocument d = new SolrInputDocument(reusedMap);

      public MapIterReader(Listener listener) {
          this.docSink = listener;
      }

      @Override
      public List<Object> readIterator(DataInputInputStream fis) throws IOException {
          docSink.start();
          try {
              while (true) {
                  Object o = readVal(fis);
                  if (o == END_OBJ) break;
                  else if (o == reusedMap) {
                    docSink.doc(d);
                  }
              }
              return null;
          } finally {
              docSink.end();

          }
      }
      @Override
      @SuppressWarnings({"unchecked"})
      protected Map<Object,Object> newMap(int size) {
          nl.clear();
          return reusedMap;
      }
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


      interface Listener {
          void start() throws IOException;
          void doc(SolrInputDocument nl) throws IOException;
          void end() throws IOException;
      }
  }


}