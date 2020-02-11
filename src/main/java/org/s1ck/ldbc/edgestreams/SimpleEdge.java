package org.s1ck.ldbc.edgestreams;

public class SimpleEdge {
    private long src;
    private long dst;

    public SimpleEdge(long src, long dst) {
        this.src = src;
        this.dst = dst;
    }

    @Override
    public String toString() {
        return "src=" + src + "|dst=" + dst;
    }
}
