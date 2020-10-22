package com.airbnb.reair.batch.template;

import org.apache.velocity.VelocityContext;
import org.apache.velocity.app.VelocityEngine;
import org.apache.velocity.exception.MethodInvocationException;
import org.apache.velocity.exception.ParseErrorException;
import org.apache.velocity.exception.ResourceNotFoundException;
import org.apache.velocity.runtime.RuntimeConstants;
import org.apache.velocity.runtime.resource.loader.ClasspathResourceLoader;

import java.io.StringWriter;

/**
 * Utilities for working with Velocity templates.
 */
public class VelocityUtils {
  /**
   * Return the String representation of the template rendered using Velocity.
   *
   * @param context context use to render the template
   * @param templateFileName file name of the template in the classpath
   * @throws TemplateRenderException if there is an error with the template
   */
  public static String renderTemplate(String templateFileName,
                                      VelocityContext context)
      throws TemplateRenderException {
    VelocityEngine ve = new VelocityEngine();
    ve.setProperty(RuntimeConstants.RESOURCE_LOADER, "classpath");
    ve.setProperty("classpath.resource.loader.class",
        ClasspathResourceLoader.class.getName());

    StringWriter sw = new StringWriter();

    try {
      ve.mergeTemplate(templateFileName, "UTF-8", context, sw);
    } catch (ResourceNotFoundException
        | ParseErrorException
        | MethodInvocationException e) {
      throw new TemplateRenderException("Error rendering template file: " + templateFileName, e);
    }

    return sw.toString();
  }

}
