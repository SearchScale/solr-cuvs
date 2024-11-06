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
import org.apache.lucene.document.Field;
import org.apache.lucene.document.KnnFloatVectorField;
import org.apache.lucene.document.StringField;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.VectorSimilarityFunction;
import org.apache.solr.client.solrj.request.JavaBinUpdateRequestCodec;
import org.apache.solr.client.solrj.request.UpdateRequest;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.common.util.ContentStream;
import org.apache.solr.common.util.FastInputStream;
import org.apache.solr.common.util.JavaBinCodec;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.core.SolrCore;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.response.SolrQueryResponse;
import org.apache.solr.security.AuthorizationContext;
import org.apache.solr.update.CommitUpdateCommand;
import org.apache.solr.util.RefCounted;
import org.apache.solr.util.plugin.SolrCoreAware;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.lang.invoke.MethodHandles;
import java.util.Collections;
import java.util.Iterator;
import java.util.Map;
import java.util.function.BiConsumer;
import java.util.function.Consumer;

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

  @Override
  @SuppressWarnings({"unchecked"})
  public void handleRequestBody(SolrQueryRequest req, SolrQueryResponse rsp) throws Exception {
    SolrParams params = req.getParams();
    ContentStream stream = extractSingleContentStream(req);

    FastInputStream in = FastInputStream.wrap(stream.getStream());
    for (; ; ) {
      if (in.peek() == -1) return;
      try {
        new JavaBinUpdateRequestCodec().unmarshal(in, new JavaBinUpdateRequestCodec.StreamingMapHandler() {
          @Override
          public void update(Map<String, Object> m, UpdateRequest req, Integer commitWithin) {
            Document d =new Document();
            m.forEach((s, o) -> {
              if (o instanceof float[]) {
                float[] floats = (float[]) o;
                d.add(new KnnFloatVectorField(s, floats, VectorSimilarityFunction.EUCLIDEAN));
              } else {
                d.add(new StringField(s, o.toString(), Field.Store.YES));
              }
            });
            try {
              write2Index(d);
            } catch (IOException e) {
              throw new RuntimeException(e);
            }

          }

          @Override
          public void update(SolrInputDocument document, UpdateRequest req, Integer commitWithin, Boolean override) {
            //not supported
          }
        });
        if(req.getParams().getBool("commit", false )){
          core.getUpdateHandler().commit(new CommitUpdateCommand(req, false));

        }
      } catch (EOFException e) {
        break; // this is expected
      }

    }


  }
  private void write2Index(Document d) throws IOException {
    RefCounted<IndexWriter> iw = core.getSolrCoreState().getIndexWriter(core);
    IndexWriter writer = iw.get();
    try {
      writer.addDocuments(Collections.singleton(d));
    } finally {
      iw.decref();
    }
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



  private static void write2Index(Document d, RefCounted<IndexWriter> iw) {
    IndexWriter writer = iw.get();
    try {
      writer.addDocuments(Collections.singleton(d));
    } catch (IOException e) {
      throw new RuntimeException(e);
    } finally {
      iw.decref();
    }
  }
}