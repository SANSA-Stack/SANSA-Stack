package net.sansa_stack.hadoop.core;


import java.io.ByteArrayInputStream;
import java.io.Closeable;
import java.io.IOException;
import java.io.InputStream;
import java.util.Arrays;
import java.util.Map;
import java.util.Map.Entry;
import java.util.NavigableMap;
import java.util.TreeMap;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.aksw.commons.io.buffer.array.ArrayOps;
import org.aksw.commons.io.buffer.array.BufferOverReadableChannel;
import org.aksw.commons.io.hadoop.SeekableInputStream;
import org.aksw.commons.io.hadoop.SeekableInputStreams;
import org.aksw.commons.io.input.ReadableChannel;
import org.aksw.commons.io.input.ReadableChannelWithConditionalBound;
import org.aksw.commons.io.input.ReadableChannels;
import org.aksw.commons.io.input.SeekableReadableChannel;
import org.aksw.commons.io.input.SeekableReadableChannelBase;
import org.aksw.commons.io.input.SeekableReadableChannelSource;
import org.aksw.commons.io.input.SeekableReadableChannelWithLimit;
import org.aksw.commons.io.input.SeekableReadableChannels;
import org.aksw.commons.util.lock.LockUtils;
import org.apache.hadoop.fs.Seekable;

import com.google.common.primitives.Ints;

import net.sansa_stack.hadoop.util.DeferredSeekablePushbackInputStream;

public class SeekableSourceOverSplit
    implements SeekableReadableChannelSource<byte[]>, Closeable
{
    @Override
    public void close() throws IOException {
        if (headBuffer.getDataSupplier() != null && headBuffer.getDataSupplier().isOpen()) {
            headBuffer.getDataSupplier().close();
        }
        if (tailBuffer.getDataSupplier() != null && tailBuffer.getDataSupplier().isOpen()) {
            tailBuffer.getDataSupplier().close();
        }
        if (postambleBuffer.getDataSupplier() != null&& postambleBuffer.getDataSupplier().isOpen()) {
            postambleBuffer.getDataSupplier().close();
        }
        if (debufferedHead != null) {
            debufferedHead.close();
        }
    }
    // protected SeekableReadableChannel<byte[]> base;

    /** The total number of bytes that need to be read from base until the split boundary is reached.
     * A value of -1 indicates unknown. For non-encoded streams this is simply the length of the split. */
    // protected long knownDecodedDataLength; // [] = new long[]{ isEncoded ? -1 : splitLength };

    // The head stream has a conditional bound at the split end
    protected BufferOverReadableChannel<byte[]> headBuffer;
    protected BufferOverReadableChannel<byte[]> tailBuffer;

    /**
     * The postamble buffer is only served if a limit is set via {@link Channel#setLimit(long)}
     * If no limit is set then the remainder of the stream is consumed which is assumed to include the postamble
     */
    protected BufferOverReadableChannel<byte[]> postambleBuffer;


    protected SeekableReadableChannel<byte[]> debufferedHead;


    /* A later stream with the same offset overrides a prior one (implies that the prior one was empty). */
    protected NavigableMap<Long, Integer> posToIndex = new TreeMap<>();

    protected NavigableMap<Long, Long> absPosToBlockOffset = null;

//    protected boolean isEndReached = false;
    

    public long getBlockForPos(long pos) {
        Map.Entry<Long, Long> e = absPosToBlockOffset.floorEntry(pos);
        // absPosToBlockOffset.headMap(pos, true).size();
        return e.getValue();
    }

//    public boolean isEndReached() {
//    	return isEndReached;
//    }
    
    public long getKnownSize() {
        Entry<Long, Integer> offsetAndBufferId = posToIndex.lastEntry();
        long bufferSize = getBufferByIndexUnsafe(offsetAndBufferId.getValue()).getKnownDataSize();

        long result = offsetAndBufferId.getKey() + bufferSize;
        return result;
    }

    /** If true then the headStream can no longer be used. */
    // protected boolean isHeadDebuffered;

    public SeekableSourceOverSplit(BufferOverReadableChannel<byte[]> headBuffer, BufferOverReadableChannel<byte[]> tailBuffer, BufferOverReadableChannel<byte[]> postambleBuffer, NavigableMap<Long, Long> absPosToBlockOffset) {
        super();
        this.headBuffer = headBuffer;
        this.tailBuffer = tailBuffer;
        this.postambleBuffer = postambleBuffer;
        this.absPosToBlockOffset = absPosToBlockOffset;
        this.posToIndex.put(0l, 0);
    }

    /**
     * @return null if the underlying stream is not based on blocks; otherwise a map of byte-offsets (staring from zero) to block offsets
     */
    public NavigableMap<Long, Long> getAbsPosToBlockOffset() {
        return absPosToBlockOffset;
    }

    protected BufferOverReadableChannel<byte[]> getBufferByBaseOffset(long baseOffset) {
        Integer index = posToIndex.get(baseOffset);
        return getBufferByIndex(index);
    }


    protected BufferOverReadableChannel<byte[]> getBufferByIndex(int index) {
        // Sanity check
        if (index == 0 && debufferedHead != null) {
            throw new IllegalStateException("Should never be called if in debuffered state");
        }
        return getBufferByIndexUnsafe(index);
    }


    protected BufferOverReadableChannel<byte[]> getBufferByIndexUnsafe(int index) {
        BufferOverReadableChannel<byte[]> result;
        switch (index) {
            case 0: result = headBuffer; break;
            case 1: result = tailBuffer; break;
            case 2: result = postambleBuffer; break;
            default: result = null; break;
        }
        return result;
    }


    protected void setupTailBuffer() {
        Map.Entry<Long, Integer> e = posToIndex.descendingMap().entrySet().iterator().next();
        long currentOffset = e.getKey();
        int currentIndex = e.getValue();

        // Assertion
        if (currentIndex != 0) {
            throw new IllegalStateException("Method may only be called during reads from the head buffer");
        }

        int nextIndex = currentIndex + 1;
        BufferOverReadableChannel<byte[]> nextBuffer = getBufferByIndex(nextIndex);
        if (nextBuffer != null) {
            BufferOverReadableChannel<byte[]> currentBuffer = getBufferByIndex(currentIndex);
            boolean doSanityCheck = true;
            if (doSanityCheck) {
                if (!currentBuffer.isDataSupplierConsumed()) {
                    throw new IllegalStateException("Attempt to set up the next buffer although the current one has not been exhausted.");
                }
            }

            long currentSize = currentBuffer.getKnownDataSize();
            long nextOffset = currentOffset + currentSize;
            posToIndex.put(nextOffset, nextIndex);
        }
    }

    public BufferOverReadableChannel<byte[]> getHeadBuffer() {
        return headBuffer;
    }

    public BufferOverReadableChannel<byte[]> getTailBuffer() {
        return tailBuffer;
    }

    public static SeekableSourceOverSplit createForNonEncodedStream(SeekableInputStream in, long splitPoint, byte[] postambleBytes) {
        SeekableReadableChannel<byte[]> baseStream = SeekableInputStreams.wrap(in);
        SeekableReadableChannel<byte[]> headStream = new SeekableReadableChannelWithLimit<>(baseStream, splitPoint);

        return create(baseStream, headStream, postambleBytes, null);
    }
    public static SeekableSourceOverSplit createForBlockEncodedStream(SeekableInputStream inn, long splitPoint, byte[] postambleBytes) {
        NavigableMap<Long, Long> absPosToBlockOffset = new TreeMap<>();

        // Not ideal to use the position without a guaranteed prior read
        absPosToBlockOffset.put(0l, inn.position());
        System.err.println("Initial block: " + absPosToBlockOffset);

        // Wrap the input stream such that the position always refers to the next byte being read
        InputStream in1 = new DeferredSeekablePushbackInputStream(inn) {
            protected long readCount = 0;

            @Override
            protected int readInternal(byte[] b, int off, int len) throws IOException {
                long before = inn.position();
                int result = super.readInternal(b, off, len);
                long after = inn.position();

                if (after != before) {
                    // System.err.println("Block detected: " + after + " -> " + readCount);
                    absPosToBlockOffset.put(readCount, after);
                }
                if (result > 0) {
                    readCount += result;
                }
                return result;
            }
        };

        // We need the position() functionality of the baseStream - but we won't be using its seeking capabilities
        SeekableReadableChannel<byte[]> baseStream = SeekableInputStreams.wrap(SeekableInputStreams.create(in1, (Seekable)in1));


        // SeekableReadableChannel<byte[]> base = SeekableReadableChannel(dataSupplier);

        // long initialPos = baseStream.position();
        // long initialSplitId = posToSplitId.apply(initialPos);

        // Wrap the stream that when reading past the split point any data is buffered with the tailBuffer
        ReadableChannel<byte[]> headStream = new ReadableChannelWithConditionalBound<>(baseStream,
            self -> {
                long pos = baseStream.position();
                // long splitId = posToSplitId.apply(pos);
                boolean isEof = pos >= splitPoint;
                if (isEof) {
                    System.err.println("Found first block after split " + splitPoint + " at " + pos);
                }
                return isEof;
            });

        return create(baseStream, headStream, postambleBytes, absPosToBlockOffset);
    }

    protected static SeekableSourceOverSplit create(
            ReadableChannel<byte[]> baseStream, ReadableChannel<byte[]> headStream, byte[] postambleBytes, NavigableMap<Long, Long> blockOffsetToAbsPos) {
        BufferOverReadableChannel<byte[]> headBuffer = BufferOverReadableChannel.createForBytes(headStream, 8192);
        BufferOverReadableChannel<byte[]> tailBuffer = BufferOverReadableChannel.createForBytes(baseStream, 8192);
        BufferOverReadableChannel<byte[]> postambleBuffer = BufferOverReadableChannel.createForBytes(ReadableChannels.wrap(new ByteArrayInputStream(postambleBytes)), 8192);
        return new SeekableSourceOverSplit(headBuffer, tailBuffer, postambleBuffer, blockOffsetToAbsPos);
    }

    public long getHeadSize() {
        long index = posToIndex.entrySet().stream()
                .filter(e -> e.getValue() == 1)
                .map(Map.Entry::getKey)
                .findFirst()
                .orElseThrow(() -> new IllegalStateException("Head size not yet detected"));
        return index;
    }

    @Override
    public Channel newReadableChannel() throws IOException {

        if (debufferedHead != null) {
            throw new RuntimeException("Already debuffered");
        }

        // We cannot use the util method BufferOverReadableChannel.newBufferedChannel because
        //  the resulting channel is not seekable...

        // BufferOverReadableChannel.newBufferedChannel(headBuffer);
        // headBuffer.newReadableChannel()
        return new Channel(headBuffer.newReadableChannel(), 0, -1, null);
    }

//    @Override
//    public SeekableReadableChannel<byte[]> newReadableChannel(long offset) throws IOException {
//        return ne
//    }

    @Override
    public long size() throws IOException {
        return headBuffer.getKnownDataSize() + tailBuffer.getKnownDataSize();
    }

    @Override
    public ArrayOps<byte[]> getArrayOps() {
        return ArrayOps.BYTE;
    }

    class Channel
        extends SeekableReadableChannelBase<byte[]>
    {
        protected SeekableReadableChannel<byte[]> currentStream;
        // protected boolean isHeadStream;

        // The offset at which the currentStream starts
        // protected int currentStreamId;
        protected long currentStreamOffset;

        protected long requestedPos;

        // protected long limitPos;

        protected Runnable transitionAction;

        protected ReentrantReadWriteLock rwl = new ReentrantReadWriteLock();

        public Channel(SeekableReadableChannel<byte[]> currentStream, long currentStreamOffset, long requestedPos, Runnable transitionAction) {
            this.currentStream = currentStream;
            this.currentStreamOffset = currentStreamOffset;
            this.requestedPos = requestedPos;
            this.transitionAction = transitionAction;
        }

        public ReadWriteLock getReadWriteLock() {
            return rwl;
        }

        /** True iff the next call to read() reads from the head stream */
        public boolean isHeadStream() {
            int streamId = posToIndex.get(currentStreamOffset);
            boolean result = streamId == 0;
            return result;
        }

        protected boolean isDebuffered() {
            return debufferedHead != null;
        }

        public void debufferHead() {
            if (!rwl.isWriteLocked()) {
                throw new IllegalStateException("Debuffering requires the channel's write lock to be locked");
            }

            if (isDebuffered()) {
                throw new RuntimeException("Already debuffered");
            }

            if (isHeadStream()) {
                long pos = position();
                long bufferSize = headBuffer.getKnownDataSize();
                ReadableChannel<byte[]> bufferChannel;

                ReadableChannel<byte[]> headDataSupplier = headBuffer.getDataSupplier();
                headBuffer.setDataSupplier(null); // TODO Set a always failing one because it should no longer be used.
                try {
                    bufferChannel = pos < bufferSize
                            ? headBuffer.getBuffer().newReadableChannel(pos)
                            : null;
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }


                ReadableChannel debuffered = bufferChannel == null
                        ? headDataSupplier
                        : ReadableChannels.concat(Arrays.asList(bufferChannel, headDataSupplier));

                debufferedHead = SeekableReadableChannels.wrapForwardSeekable(debuffered, pos);
                currentStream = debufferedHead;
            }
            // BufferOverReadableChannel.newBufferedChannel(headBuffer);
        }

        @Override
        public SeekableReadableChannel<byte[]> cloneObject() {
            try {
                long pos = position();
                return new Channel(currentStream.cloneObject(), currentStreamOffset, pos, transitionAction);
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }

        @Override
        public long position() {
            long result = requestedPos >= 0 ? requestedPos : getInternalPosition();
            return result;
        }

        @Override
        public void position(long pos) {
            this.requestedPos = pos;
        }


        protected void applyPosition() throws IOException {
            long currentAbsPos = getInternalPosition();

            while (true) {
                long requestedBaseOffset = posToIndex.floorKey(requestedPos);
                Integer requestedIndex = posToIndex.get(requestedBaseOffset);

                long requiredAdditionalBytes = requestedPos - currentAbsPos;
                long currentRelPos = requestedPos - requestedBaseOffset;

                if (requestedIndex == 0 && isDebuffered()) {
                    System.err.println("Debuffered stream pos:" + currentStream.position());
                    System.err.println("Requested pos: " + currentRelPos);
                    currentStream.position(currentRelPos);
                    break;
                    // currentStreamOffset = 0;
                } else {

                    BufferOverReadableChannel requestedBuffer = getBufferByBaseOffset(requestedBaseOffset);
                    if (requestedBaseOffset != currentStreamOffset) {
                        currentStream.close();
                        // currentStream = BufferOverReadableChannel.newBufferedChannel(currentBuffer);
                        currentStream = requestedBuffer.newReadableChannel();
                        currentStreamOffset = requestedBaseOffset;
                    }

                    if (requiredAdditionalBytes > 0) {
                        // TODO Make loadFully accept a long argument
                        requestedBuffer.loadFully(Ints.checkedCast(currentRelPos), true);
                    }

                    long knownDataSize = requestedBuffer.getKnownDataSize();
                    if (currentRelPos < knownDataSize || (currentRelPos == knownDataSize && !requestedBuffer.isDataSupplierConsumed())) {
                        currentStream.position(currentRelPos);
                        break;
                    } else {
                        int currentStreamIdx = posToIndex.get(currentStreamOffset);
                        if (currentStreamIdx == 0 && requestedBuffer.isDataSupplierConsumed()) {
                            setupTailBuffer();
                        }

                        long nextRequestedBaseOffset = posToIndex.floorKey(requestedPos);
                        if (requestedBaseOffset == nextRequestedBaseOffset) {
                            currentStream.position(knownDataSize);
                            break;
                        } else {
                            continue;
                        }
                    }
                }
            }

            requestedPos = -1;
        }

        protected long getInternalPosition() {
            long relativePos = currentStream.position();
            long result = currentStreamOffset + relativePos;
            return result;
        }

        void setLimit(long newLimitPos) {
            // int size = posToIndex.size();
            int max = posToIndex.values().stream().mapToInt(x -> x).max().orElse(-1);
            if (max != 1) {
                throw new IllegalStateException("Limit can only be set once and only if data has been read from the tail region");
            }
//            if (limitPos != -1) {
//                throw new RuntimeException(String.format("Cannot re-set limit from %d to %d", limitPos, newLimitPos));
//            }
            // this.limitPos = newLimitPos;
            posToIndex.put(newLimitPos, 2);
        }

        @Override
        public int read(byte[] array, int position, int length) throws IOException {
            int result;
            if (length == 0) {
                result = 0;
            } else {
                Lock readLock = rwl.readLock();
                readLock.lock();
                try {
                    while (true) {
                        if (requestedPos >= 0) {
                            applyPosition();
                        }

                        int l = adjustLength(length);
                        if (l <= 0) {
                            long p = position();
                            position(p);
                            continue;
                        } else {
                            result = currentStream.read(array, position, l);
                            if (result == -1) {
                                Object cs = currentStream;
                                long currentSize = isHeadStream() && isDebuffered()
                                        ? currentStream.position() - currentStreamOffset
                                        : getBufferByBaseOffset(currentStreamOffset).getKnownDataSize();

//                                long csPos = currentStream.position();
//                                long currentSize = csPos - currentStreamOffset; // getBufferByBaseOffset(currentStreamOffset).getKnownDataSize();


                                boolean exhaustedHeadStream = isHeadStream();
                                long newPos = currentStreamOffset + currentSize;
                                position(newPos);
                                if (exhaustedHeadStream) {
                                    posToIndex.put(newPos, 1);
                                    // setupTailBuffer();
                                }
                                applyPosition();

                                // If we did not move to a new stream then we reached the end
                                if (currentStream == cs) {
                                    break;
                                }

                                if (exhaustedHeadStream) {
                                    // currentStream.close();
                                    // isHeadStream = false;
                                    // currentStream = tailBuffer.newReadableChannel();

                                    transition();
                                }
                                continue;
                                // l = adjustLength(length);
                                // result = l <= 0 ? (length > 0 ? -1 : 0) : currentStream.read(array, position, l);
                            }
                            // requestedPos = -1; // getInternalPosition();
                        }
                        break;
                    }
                } finally {
                    readLock.unlock();
                }
            }
            if (result == -1) {
            	// isEndReached = true;            	
                // System.out.println("EOF reached");
            }
            return result;
        }


        public int adjustLength(int length) {
            Long nextStreamOffset = posToIndex.higherKey(currentStreamOffset);
            int l;
            if (nextStreamOffset == null) {
                l = length;
            } else {
                long p = position();
                long delta = nextStreamOffset - p;
                l = Math.min(length, Ints.saturatedCast(delta));
            }
            return l;

//            int l;
//            if (limitPos < 0) {
//                l = length;
//            } else {
//                long p = position();
//                long delta = limitPos - p;
//                l = Math.min(length, Ints.saturatedCast(delta));
//            }
//            return l;
        }

        @Override
        public ArrayOps<byte[]> getArrayOps() {
            return ArrayOps.BYTE;
        }

        @Override
        protected void closeActual() throws Exception {
            LockUtils.runWithLock(rwl.writeLock(), () -> {
                currentStream.close();
                super.closeActual();
            });
        }

        public void setTransitionAction(Runnable transitionAction) {
            this.transitionAction = transitionAction;
        }

        protected void transition() {
            if (transitionAction != null) {
                transitionAction.run();
            }
        }

        SeekableSourceOverSplit getEnclosingInstance() {
            return SeekableSourceOverSplit.this;
        }
    }
}
