package net.sansa_stack.hadoop.core;

import org.aksw.jenax.annotation.reprogen.IriNs;
import org.aksw.jenax.annotation.reprogen.Namespace;
import org.apache.jena.rdf.model.Resource;

@Namespace("http://www.example.org/")
public interface Stats2
    extends Resource
{
    @IriNs
    Long getSplitStart();
    Stats2 setSplitStart(Long value);

    /** Null if the split is not backed by a blocked stream; -1 if the stream uses blocks but none was detected */
    @IriNs
    Long getFirstBlock();
    Stats2 setFirstBlock(Long value);

    /** Note: The size is probably more helpful than its absolute end offset */
    @IriNs
    Long getSplitSize();
    Stats2 setSplitSize(Long value);

    @IriNs
    Boolean isRegionStartSearchReadOverSplitEnd();
    Stats2 setRegionStartSearchReadOverSplitEnd(Boolean value);

    @IriNs
    Boolean isRegionStartSearchReadOverRegionEnd();
    Stats2 setRegionStartSearchReadOverRegionEnd(Boolean value);

    @IriNs
    Long getTotalElementCount();
    Stats2 setTotalElementCount(Long value);

    @IriNs
    Integer getTailElementCount();
    Stats2 setTailElementCount(Integer value);

    @IriNs
    Long getRecordCount();
    Stats2 setRecordCount(Long value);

    @IriNs
    Long getTotalRecordCount();
    Stats2 setTotalRecordCount(Long value);

    @IriNs
    ProbeStats getRegionStartProbeResult();
    Stats2 setRegionStartProbeResult(ProbeStats value);

    @IriNs
    ProbeStats getRegionEndProbeResult();
    Stats2 setRegionEndProbeResult(ProbeStats value);

    @IriNs
    Long getTotalBytesRead();
    Stats2 setTotalBytesRead(Long value);

    @IriNs
    Double getTotalTime();
    Stats2 setTotalTime(Double value);

    @IriNs
    String getErrorMessage();
    Stats2 setErrorMessage(String value);
}
