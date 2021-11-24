package org.apache.jena.system;

// HACK because commons-rdf (and maybe elephas) are not available in Jena 4
public class JenaSystem {
    public static void init() {
        org.apache.jena.sys.JenaSystem.init();
    }
}

