package net.sansa_stack.query.spark.engine;

import org.apache.jena.sparql.algebra.Op;
import org.apache.jena.sparql.algebra.OpVisitor;
import org.apache.jena.sparql.algebra.op.*;
import org.apache.jena.sparql.engine.binding.Binding;
import org.apache.spark.api.java.JavaRDD;

import java.util.ArrayDeque;
import java.util.Deque;

public class ExecutionDispatch
        implements OpVisitor {
    protected OpExecutor opExecutor;

    public ExecutionDispatch(OpExecutor opExecutor) {
        this.opExecutor = opExecutor;
    }

    protected Deque<JavaRDD<Binding>> stack = new ArrayDeque<>();

    public JavaRDD<Binding> exec(Op op, JavaRDD<Binding> input) {
        stack.push(input);
        int x = stack.size();
        op.visit(this);
        int y = stack.size();
        if (x != y) throw new RuntimeException("Possible stack misalignment");
        JavaRDD<Binding> result = stack.pop();
        return result;
    }

    @Override
    public void visit(OpBGP opBGP) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void visit(OpQuadPattern quadPattern) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void visit(OpQuadBlock quadBlock) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void visit(OpTriple opTriple) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void visit(OpQuad opQuad) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void visit(OpPath opPath) {
        throw new UnsupportedOperationException();
    }

    // @Override public void visit(opFind: OpFind){ throw new UnsupportedOperationException(); }

    @Override
    public void visit(OpTable opTable) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void visit(OpNull opNull) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void visit(OpProcedure opProc) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void visit(OpPropFunc opPropFunc) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void visit(OpFilter opFilter) {
        stack.push(opExecutor.execute(opFilter, stack.pop()));
    }

    @Override
    public void visit(OpGraph opGraph) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void visit(OpService opService) {
        stack.push(opExecutor.execute(opService, stack.pop()));
    }

    @Override
    public void visit(OpDatasetNames dsNames) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void visit(OpLabel opLabel) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void visit(OpAssign opAssign) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void visit(OpExtend opExtend) { stack.push(opExecutor.execute(opExtend, stack.pop())); }

    @Override
    public void visit(OpJoin opJoin) { stack.push(opExecutor.execute(opJoin, stack.pop())); }

    @Override
    public void visit(OpLeftJoin opLeftJoin) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void visit(OpUnion opUnion) {
        stack.push(opExecutor.execute(opUnion, stack.pop()));
    }

    @Override
    public void visit(OpDiff opDiff) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void visit(OpMinus opMinus) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void visit(OpConditional opCondition) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void visit(OpSequence opSequence) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void visit(OpDisjunction opDisjunction) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void visit(OpList opList) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void visit(OpOrder opOrder) {
        stack.push(opExecutor.execute(opOrder, stack.pop()));
    }

    @Override
    public void visit(OpProject opProject) {
        stack.push(opExecutor.execute(opProject, stack.pop()));
    }

    @Override
    public void visit(OpReduced opReduced) {
        stack.push(opExecutor.execute(opReduced, stack.pop()));
    }

    @Override
    public void visit(OpDistinct opDistinct) {
        stack.push(opExecutor.execute(opDistinct, stack.pop()));
    }

    @Override
    public void visit(OpSlice opSlice) {
        stack.push(opExecutor.execute(opSlice, stack.pop()));
    }


    @Override
    public void visit(OpGroup opGroup) {
        stack.push(opExecutor.execute(opGroup, stack.pop()));
    }

    @Override
    public void visit(OpTopN opTop) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void visit(OpLateral opLateral) { stack.push(opExecutor.execute(opLateral, stack.pop())); }
}
