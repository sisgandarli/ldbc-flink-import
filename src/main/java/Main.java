import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.s1ck.ldbc.LDBCToFlink;
import org.s1ck.ldbc.tuples.LDBCEdge;

public class Main {
    public static void main(String[] args) throws Exception {
        String path = System.getProperty("user.dir") + "/social_network/static";
        LDBCToFlink ldbcToFlink = new LDBCToFlink(path, StreamExecutionEnvironment.getExecutionEnvironment());
        DataStream<LDBCEdge> edges = ldbcToFlink.getEdges();
        edges.print();
    }
}