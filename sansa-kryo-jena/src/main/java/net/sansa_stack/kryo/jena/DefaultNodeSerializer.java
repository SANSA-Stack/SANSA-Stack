package net.sansa_stack.kryo.jena;

/**
 * A helper class in order to allow globally changing the node serialization strategy
 * in one place. This class was introduced because certain pieces of code refer to
 * a NodeSerializer however there are different ways this could be implemented.
 *
 * @author Claus Stadler
 */
public class DefaultNodeSerializer
    extends GenericNodeSerializerCustom
{
    public DefaultNodeSerializer() {
        super();
    }
}
