package com.airbnb.reair;

import static spark.Spark.get;
import static spark.Spark.port;

import org.apache.commons.cli.BasicParser;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.Options;
import org.apache.thrift.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import spark.ModelAndView;

import java.util.HashMap;
import java.util.Map;

public class WebServer {

  private static Logger LOG = LoggerFactory.getLogger(WebServer.class);

  /**
   * TODO. Warning suppression needed for the OptionBuilder API
   *
   * @param argv TODO
   *
   * @throws Exception TODO
   */
  @SuppressWarnings("static-access")
  public static void main(final String[] argv) throws Exception {
    Options options = new Options();

    options.addOption(OptionBuilder.withLongOpt("thrift-host")
        .withDescription("Host name for the thrift service").hasArg().withArgName("HOST").create());

    options.addOption(OptionBuilder.withLongOpt("thrift-port")
        .withDescription("Port for the thrift service").hasArg().withArgName("PORT").create());

    options.addOption(OptionBuilder.withLongOpt("http-port")
        .withDescription("Port for the HTTP service").hasArg().withArgName("PORT").create());


    CommandLineParser parser = new BasicParser();
    CommandLine cl = parser.parse(options, argv);

    String thriftHost = "localhost";
    int thriftPort = 9996;
    int httpPort = 8080;

    if (cl.hasOption("thrift-host")) {
      thriftHost = cl.getOptionValue("thrift-host");
      LOG.info("thriftHost=" + thriftHost);
    }

    if (cl.hasOption("thrift-port")) {
      thriftPort = Integer.valueOf(cl.getOptionValue("thrift-port"));
      LOG.info("thriftPort=" + thriftPort);
    }

    if (cl.hasOption("http-port")) {
      httpPort = Integer.valueOf(cl.getOptionValue("http-port"));
      LOG.info("httpPort=" + httpPort);
    }

    port(httpPort);

    final String finalThriftHost = thriftHost;
    final int finalThriftPort = thriftPort;

    LOG.info(String.format("Connecting to thrift://%s:%s and " + "serving HTTP on %s",
        finalThriftHost, finalThriftPort, httpPort));
    get("/jobs", (request, response) -> {
      PageData pd = new PageData(finalThriftHost, finalThriftPort);

      boolean dataFetchSuccessful = false;
      try {
        pd.fetchData();
        dataFetchSuccessful = true;
      } catch (TException e) {
        LOG.error("Error while fetching data!", e);
      }

      Map<String, Object> model = new HashMap<>();
      model.put("host", finalThriftHost);
      model.put("port", finalThriftPort);
      model.put("data_fetch_successful", dataFetchSuccessful);
      model.put("lag_in_minutes", pd.getLag() / 1000 / 60);
      model.put("active_jobs", pd.getActiveJobs());
      model.put("retired_jobs", pd.getRetiredJobs());
      model.put("date_utils", new DateUtils());
      return new ModelAndView(model, "jobs");
    } , new ThymeleafTemplateEngine());

  }
}
