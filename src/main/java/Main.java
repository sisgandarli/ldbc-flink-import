import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.s1ck.ldbc.LDBCToFlink;
import org.s1ck.ldbc.edgestreams.SimpleEdge;
import org.s1ck.ldbc.edgestreams.SimpleEdgeStream;
import org.s1ck.ldbc.tuples.LDBCEdge;

import java.io.PrintStream;

public class Main {
    public static void main(String[] args) throws Exception {
        String path = "/home/shahnur/Desktop/testdata/";
        String path1 = "/home/shahnur/Desktop/github_repos/ldbc_snb_datagen/social_network/dynamic/";
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        SimpleEdgeStream simpleEdgeStream = new SimpleEdgeStream(path1, env);
        DataStream<SimpleEdge> edges = simpleEdgeStream.getSimpleEdges();
        edges.print();
        env.execute();

    }
}