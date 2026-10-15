package com.authzed.examples.materialize;

import java.util.Properties;

/** Destination extension point. Factory is constructed once for each non-overlapping stream attempt. */
public interface BatchSinkFactory {
    BatchSink create(Properties configuration) throws Exception;

    interface BatchSink extends AsyncMaterializeConsumer.Publisher, AutoCloseable {
        /** Stop/finish all prior sends and completion callbacks before returning; do not reconnect sooner. */
        @Override void close() throws Exception;
    }
}
