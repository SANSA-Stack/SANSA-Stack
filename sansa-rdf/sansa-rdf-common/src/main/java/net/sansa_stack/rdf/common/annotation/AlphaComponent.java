package net.sansa_stack.rdf.common.annotation;

import java.lang.annotation.*;

/**
 * A new component of a SANSA Stack layer which may have unstable API's.
 * <p>
 * NOTE: If there exists a Scaladoc comment that immediately precedes this annotation, the first
 * line of the comment must be ":: AlphaComponent ::" with no trailing blank line. This is because
 * of the known issue that Scaladoc displays only either the annotation or the comment, whichever
 * comes first.
 */
@Retention(RetentionPolicy.RUNTIME)
@Target({ElementType.TYPE, ElementType.FIELD, ElementType.METHOD, ElementType.PARAMETER,
        ElementType.CONSTRUCTOR, ElementType.LOCAL_VARIABLE, ElementType.PACKAGE})
public @interface AlphaComponent {}