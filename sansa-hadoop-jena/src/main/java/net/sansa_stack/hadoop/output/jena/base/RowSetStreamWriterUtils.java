package net.sansa_stack.hadoop.output.jena.base;

import org.aksw.jenax.io.rowset.core.RowSetStreamWriter;

import java.io.IOException;

public class RowSetStreamWriterUtils {

    /**
     *  Based on fragmentOutputSpec does the following:
     *  <ul>
     *      <li>If this is the first partition then writes the header. Returns false to indicate that no previous binding exists</li>
     *      <li>In any other case simply returns true.</li>
     *  </ul>
     *
     * @return Whether a previous binding is assumed to exist
     */
    public static boolean init(RowSetStreamWriter writer, FragmentOutputSpec fragmentOutputSpec) throws IOException {
        boolean hasPreviousBinding;

        if (fragmentOutputSpec.isEmitHead()) {
            writer.writeHeader();
            writer.beginBindings();
            hasPreviousBinding = false;
        } else {
            // We assume that there is a previous record
            hasPreviousBinding = true;
        }

        return hasPreviousBinding;
    }

    /*
    public static void write(RowSetStreamWriter writer, RowSet rowSet, FragmentOutputSpec fragmentOutputSpec) throws IOException {
        boolean hasPreviousBinding = init(writer,fragmentOutputSpec);

        while (rowSet.hasNext()) {
            if (hasPreviousBinding) {
                writer.writeBindingSeparator();
            }

            Binding binding = rowSet.next();
            writer.writeBinding(binding);
            hasPreviousBinding = true;
        }

        if (fragmentOutputSpec.isEmitTail()) {
            writer.endBindings();
            writer.writeFooter();
        }
    }
    */
}
