package org.apache.solr.handler;

import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.common.SolrInputField;
import org.apache.solr.common.util.DataInputInputStream;
import org.apache.solr.common.util.JavaBinCodec;
import org.apache.solr.common.util.NamedList;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.lang.invoke.MethodHandles;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;

public class JavabinBulkReader extends JavaBinCodec {
    private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

    final Listener docSink;
    ArrayBlockingQueue<SolrInputDocument> queue = new ArrayBlockingQueue<>(1000);
    Thread indexerThread;
    private static SolrInputDocument NULL_DOC = new SolrInputDocument();

    private volatile boolean isEnd = false;
    public JavabinBulkReader(Listener listener) {
        this.docSink = listener;
    }

    @Override
    public List<Object> readIterator(DataInputInputStream fis) throws IOException {
        docSink.start();
        indexerThread = new Thread(new IndexerRunnable());
        indexerThread.start();
        try {
            while (true) {
                Object o = readVal(fis);
                if (o == END_OBJ) break;
                else if (o instanceof Map) {
                    docSink.doc(new SolrInputDocument((Map)o));
                }
            }
            return null;
        } finally {
            triggerEndEvent();

        }
    }

    private void triggerEndEvent()  {
        if(!isEnd) {
            isEnd = true;
            try {
                indexerThread.join(2*60*1000);// wait for a max of 2 minutes
            } catch (InterruptedException e) {
             log.error("Unable to join() indexerThread");
            }
            try {
                queue.put(NULL_DOC);
            } catch (Exception e) {
                log.error("Can't signal end to the Indexing thread");
            }
            try {
                docSink.end();
            } catch (IOException e) {
                log.error("listener.end() caused exception ");
            }
        }
    }

    @Override
    @SuppressWarnings({"rawtypes"})
    protected Map<Object, Object> newMap(int size) {
        NamedList<Object> nl = new NamedList<>(10) {
            @Override
            public void add(String name, Object val) {
                SolrInputField f = new SolrInputField(name);
                f.setValue(val);
                super.add(name, f);
            }
        };
        Map m =nl.asShallowMap(true);
        return  m;
    }


    interface Listener {
        void start() throws IOException;

        void doc(SolrInputDocument nl) throws IOException;

        void end() throws IOException;
    }

    class IndexerRunnable implements Runnable {

        @Override
        public void run() {

            try {
                for(;;){
                        SolrInputDocument d = queue.take();
                        if(d == NULL_DOC) break;
                        docSink.doc(d);
                }
            } catch (Exception e){
             log.error("something unexpected happened", e);
            } finally {
               triggerEndEvent();

            }

        }
    }
}
