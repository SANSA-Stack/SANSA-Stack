package net.sansa_stack.rdf.common.io.hadoop;

import java.nio.channels.spi.AbstractInterruptibleChannel;

// TODO Delete this class; but first document this comment/snippet somewhere (sansa github wiki?)
public class Trash {
    /**
     * It took one week to figure out the race condition that would indeterministically cause jena to bail out with
     * parse errors in the assembled head/body/tail buffers. So the reason is, that RDFDataMgrRx tries
     * to interrupt the producer thread of the RDF parser (if there is one). However, FileChannel
     * extends AbstractInterruptibleChannel which in turn closes itself if during read the thread is interrupted.
     *
     * https://stackoverflow.com/questions/52261086/is-there-a-way-to-prevent-closedbyinterruptexception
     */
//    def doNotCloseOnInterrupt(fc:AbstractInterruptibleChannel): Unit = {
//        try {
//            val field = classOf[AbstractInterruptibleChannel].getDeclaredField("interruptor")
//            field.setAccessible(true)
//            field.set(fc, (thread: Thread) => logger.warn(fc + " not closed on interrupt").asInstanceOf[Interruptible])
//        } catch {
//            case e: Exception =>
//                logger.warn("Couldn't disable close on interrupt", e)
//        }
//    }

}
