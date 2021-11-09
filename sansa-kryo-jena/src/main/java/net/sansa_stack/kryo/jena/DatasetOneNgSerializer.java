package net.sansa_stack.kryo.jena;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.Serializer;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import org.aksw.jenax.arq.dataset.api.DatasetOneNg;
import org.aksw.jenax.arq.dataset.impl.DatasetOneNgImpl;
import org.apache.jena.rdf.model.Model;
import org.apache.jena.rdf.model.impl.ModelCom;

public class DatasetOneNgSerializer
    extends Serializer<DatasetOneNg>
{
    @Override
    public void write(Kryo kryo, Output output, DatasetOneNg object) {
        output.writeString(object.getGraphName());
        kryo.writeObjectOrNull(output, object.getModel(), ModelCom.class);
    }

    @Override
    public DatasetOneNg read(Kryo kryo, Input input, Class<DatasetOneNg> type) {
        String graphName = input.readString();
        Model model = kryo.readObjectOrNull(input, ModelCom.class);
        return DatasetOneNgImpl.create(graphName, model.getGraph());
    }
}
