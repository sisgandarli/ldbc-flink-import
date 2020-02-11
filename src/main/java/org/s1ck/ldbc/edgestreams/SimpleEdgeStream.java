package org.s1ck.ldbc.edgestreams;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.s1ck.ldbc.LDBCToFlink;
import org.s1ck.ldbc.tuples.LDBCEdge;

public class SimpleEdgeStream extends LDBCToFlink {

    public SimpleEdgeStream(String ldbcDirectory, StreamExecutionEnvironment env) {
        super(ldbcDirectory, env);
    }

    public DataStream<SimpleEdge> getSimpleEdges() throws Exception {
        DataStream<LDBCEdge> dataStream = super.getEdges();
        return dataStream.map(ldbcEdge -> new SimpleEdge(ldbcEdge.getSourceVertexId(), ldbcEdge.getTargetVertexId()));
    }
}
