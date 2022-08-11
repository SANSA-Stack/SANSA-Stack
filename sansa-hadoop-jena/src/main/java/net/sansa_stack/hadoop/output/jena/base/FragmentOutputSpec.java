package net.sansa_stack.hadoop.output.jena.base;

public class FragmentOutputSpec {
    protected boolean emitHead;
    protected boolean emitTail;

    public FragmentOutputSpec(boolean emitHead, boolean emitTail) {
        this.emitHead = emitHead;
        this.emitTail = emitTail;
    }

    /**
     *
     * @param numSplits
     * @param splitId Must start with 0
     * @return
     */
    public static FragmentOutputSpec create(int numSplits, int splitId) {
        return new FragmentOutputSpec(splitId == 0, splitId + 1 == numSplits);
    }

    public boolean isEmitHead() {
        return emitHead;
    }

    public boolean isEmitTail() {
        return emitTail;
    }
}
