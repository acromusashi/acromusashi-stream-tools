/**
* Copyright (c) Acroquest Technology Co, Ltd. All Rights Reserved.
* Please read the associated COPYRIGHTS file for more details.
*
* THE SOFTWARE IS PROVIDED BY Acroquest Technolog Co., Ltd.,
* WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED, INCLUDING
* BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
* FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT.
* IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDER BE LIABLE FOR ANY
* CLAIM, DAMAGES SUFFERED BY LICENSEE AS A RESULT OF USING, MODIFYING
* OR DISTRIBUTING THIS SOFTWARE OR ITS DERIVATIVES.
*/
package acromusashi.stream.tools;

import java.io.IOException;
import java.text.MessageFormat;
import java.util.concurrent.TimeUnit;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.cli.PosixParser;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import acromusashi.stream.config.StormConfigGenerator;
import acromusashi.stream.config.StormConfigUtil;
import acromusashi.stream.exception.ConnectFailException;
import acromusashi.stream.topology.state.TopologyExecutionGetter;
import acromusashi.stream.topology.state.TopologyExecutionStatus;
import backtype.storm.Config;

/**
 * Topology tuple execution stop wait tool's main class.<br>
 * This tool execute following procedure.<br>
 * <ol>
 * <li>Get topology statistics from nimbus interval.</li>
 * <li>If topology statistics does not mutate in intervals, exit with ReturnCode 0.</li>
 * <li>If topology statistics mutates till timeout, exit with ReturnCode 1.</li>
 * <li>If topology not exist, or other error occured, exit with ReturnCode 2.</li>
 * </ol>
 * 
 * @author kimura
 */
public class TopologyExecutionWaitTool
{
    /** Default config path */
    private static final String DEFAULT_CONFIG_PATH        = "/opt/storm/conf/storm.yaml";

    /** Default check interval */
    private static final int    DEFAULT_INTERVAL           = 1;

    /** Default wait timeout */
    private static final int    DEFAULT_WAIT_TIMEOUT       = 30;

    /** Default nimbus thrift port */
    private static final int    DEFAULT_NIMBUS_THRIFT_PORT = 6627;

    /** Check count */
    private static final int    CHECK_COUNT                = 5;

    /** Return code Topology execution not stopped. */
    private static final int    RETURN_NOT_STOPPED         = 1;

    /** Return code Topology execution get failed. */
    private static final int    RETURN_FAILURE             = 2;

    /** Logger */
    private static final Logger logger                     = LoggerFactory.getLogger(TopologyExecutionWaitTool.class);

    /**
     * Constructor
     */
    private TopologyExecutionWaitTool()
    {
        // Do nothing.
    }

    /**
     * Program Entry Point<br>
     * <br>
     * Use following arguments.<br>
     * <ul>
     * <li>-c Config path(optional, default /opt/storm/conf/storm.yaml)</li>
     * <li>-i Topology statistics check interval(optional, default 3sec)</li>
     * <li>-t Check target topology name(requred)</li>
     * <li>-w Wait time out(optional, default 30sec)</li>
     * <li>-sh Show help</li>
     * </ul>
     * 
     * @param args Argments
     */
    public static void main(String... args)
    {
        TopologyExecutionWaitTool waitTool = new TopologyExecutionWaitTool();
        waitTool.startWaitTool(args);
    }

    /**
     * Start config put tool.<br>
     * Check arguments and start tool.
     * 
     * @param args Argments
     */
    private void startWaitTool(String[] args)
    {
        Options cliOptions = createOptions();
        CommandLineParser parser = new PosixParser();
        CommandLine commandLine = null;
        HelpFormatter help = new HelpFormatter();

        try
        {
            commandLine = parser.parse(cliOptions, args);
        }
        catch (ParseException pex)
        {
            printHelpAndExit(help, cliOptions);
            return;
        }

        if (commandLine.hasOption("sh"))
        {
            printHelpAndExit(help, cliOptions);
        }

        String configPath = DEFAULT_CONFIG_PATH;
        if (commandLine.hasOption("c") == true)
        {
            configPath = commandLine.getOptionValue("c");
        }

        int interval = DEFAULT_INTERVAL;
        if (commandLine.hasOption("i") == true)
        {
            try
            {
                interval = Integer.parseInt(commandLine.getOptionValue("i"));
            }
            catch (NumberFormatException ex)
            {
                printHelpAndExit(help, cliOptions);
            }
        }

        int wait = DEFAULT_WAIT_TIMEOUT;
        if (commandLine.hasOption("w") == true)
        {
            try
            {
                wait = Integer.parseInt(commandLine.getOptionValue("w"));
            }
            catch (NumberFormatException ex)
            {
                printHelpAndExit(help, cliOptions);
            }
        }

        String targetTopology = commandLine.getOptionValue("t");

        executeWaitTool(configPath, interval, targetTopology, wait);
    }

    /**
     * Execute wait tool.
     * 
     * @param configPath configPath
     * @param interval interval
     * @param targetTopology targetTopology
     * @param wait wait
     */
    private void executeWaitTool(String configPath, int interval, String targetTopology, int wait)
    {
        Config config = null;
        try
        {
            config = StormConfigGenerator.loadStormConfig(configPath);
        }
        catch (IOException ex)
        {
            String logFormat = "Config file load failed. Exit wait tool. : ConfigPath={0}";
            logger.error(MessageFormat.format(logFormat, configPath), ex);
            Runtime.getRuntime().exit(RETURN_FAILURE);
            return;
        }

        String nimbusHost = StormConfigUtil.getStringValue(config, Config.NIMBUS_HOST, "");
        int nimbusPort = StormConfigUtil.getIntValue(config, Config.NIMBUS_THRIFT_PORT,
                DEFAULT_NIMBUS_THRIFT_PORT);

        long startTime = System.currentTimeMillis();

        TopologyExecutionGetter getter = new TopologyExecutionGetter();
        TopologyExecutionStatus result = null;

        while (true)
        {
            try
            {
                result = getter.getExecution(targetTopology, nimbusHost, nimbusPort,
                        (int) TimeUnit.SECONDS.toMillis(interval), CHECK_COUNT);
            }
            catch (ConnectFailException ex)
            {
                String logFormat = "Nimbus connect failed. Exit wait tool. : NimbusHost={0}, NimbusPort={1}";
                logger.error(MessageFormat.format(logFormat, nimbusHost, nimbusPort), ex);
                Runtime.getRuntime().exit(RETURN_FAILURE);
                return;
            }

            if (result == TopologyExecutionStatus.STOP)
            {
                break;
            }

            if (result == TopologyExecutionStatus.NOT_ALIVED)
            {
                String logFormat = "Topology not exist. Exit wait tool. : Topology={0}";
                logger.error(MessageFormat.format(logFormat, targetTopology));
                Runtime.getRuntime().exit(RETURN_FAILURE);
                break;
            }

            if (result == TopologyExecutionStatus.EXECUTING)
            {
                long nowTime = System.currentTimeMillis();
                if ((nowTime - startTime) > TimeUnit.SECONDS.toMillis(wait))
                {
                    String logFormat = "Topology wait timeout. Exit wait tool. : Topology={0}";
                    logger.error(MessageFormat.format(logFormat, targetTopology));
                    Runtime.getRuntime().exit(RETURN_NOT_STOPPED);
                    break;
                }
            }
        }

    }

    /**
     * Print help message and exit.
     * 
     * @param help HelpFormatter
     * @param cliOptions Options
     */
    private void printHelpAndExit(HelpFormatter help, Options cliOptions)
    {
        help.printHelp(TopologyExecutionWaitTool.class.getName(), cliOptions, true);
        Runtime.getRuntime().exit(RETURN_FAILURE);
    }

    /**
     * Generate command line analyze option object.
     * 
     * @return command line analyze option object
     */
    public static Options createOptions()
    {
        Options cliOptions = new Options();

        // Config path option
        OptionBuilder.hasArg(true);
        OptionBuilder.withArgName("Storm config path");
        OptionBuilder.withDescription("Storm config path");
        OptionBuilder.isRequired(false);
        Option configOption = OptionBuilder.create("c");

        // Check interval option
        OptionBuilder.hasArg(true);
        OptionBuilder.withArgName("Check interval(Sec)");
        OptionBuilder.withDescription("Check interval(Sec)");
        OptionBuilder.isRequired(false);
        Option intervalOption = OptionBuilder.create("i");

        // Check target topology name
        OptionBuilder.hasArg(true);
        OptionBuilder.withArgName("Check target topology name");
        OptionBuilder.withDescription("Check target topology name");
        OptionBuilder.isRequired(true);
        Option targetOption = OptionBuilder.create("t");

        // Wait timeout option
        OptionBuilder.hasArg(true);
        OptionBuilder.withArgName("Wait timeout(Sec)");
        OptionBuilder.withDescription("Wait timeout(Sec)");
        OptionBuilder.isRequired(false);
        Option waitOption = OptionBuilder.create("w");

        // Help option
        OptionBuilder.withDescription("show help");
        Option helpOption = OptionBuilder.create("sh");

        cliOptions.addOption(configOption);
        cliOptions.addOption(intervalOption);
        cliOptions.addOption(targetOption);
        cliOptions.addOption(waitOption);
        cliOptions.addOption(helpOption);
        return cliOptions;
    }
}
