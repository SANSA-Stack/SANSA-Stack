package net.sansa_stack.hadoop.core;

import java.time.Duration;

import org.aksw.jenax.annotation.reprogen.IriNs;
import org.aksw.jenax.annotation.reprogen.Namespace;
import org.apache.jena.rdf.model.Resource;

@Namespace("http://www.example.org/")
public interface ProbeStats
    extends Resource
{
    @IriNs
    Long getCandidatePos();
    ProbeStats setCandidatePos(Long value);

    @IriNs
    Long getProbeCount();
    ProbeStats setProbeCount(Long value);

    @IriNs
    Double getTotalDuration();
    ProbeStats setTotalDuration(Double value);
}
