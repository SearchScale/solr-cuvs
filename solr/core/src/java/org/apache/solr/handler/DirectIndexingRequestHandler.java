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

import org.apache.solr.common.SolrException;
import org.apache.solr.common.util.ContentStream;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.core.SolrCore;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.response.SolrQueryResponse;
import org.apache.solr.security.AuthorizationContext;
import org.apache.solr.util.plugin.SolrCoreAware;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.invoke.MethodHandles;
import java.util.Iterator;

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
  public void handleRequestBody(SolrQueryRequest req, SolrQueryResponse rsp) throws Exception {
      ContentStream is = extractSingleContentStream(req);
      DocListener listener = new DocListener(req.getParams(), core);
      new JavabinBulkReader(listener).unmarshal(is.getStream());
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
    return Category.UPDATE;
  }

  @Override
  public Name getPermissionName(AuthorizationContext request) {
    return Name.UPDATE_PERM;
  }


}