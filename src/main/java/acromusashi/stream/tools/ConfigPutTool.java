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

import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.text.MessageFormat;
import java.util.List;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.cli.PosixParser;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.thrift7.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import acromusashi.stream.client.NimbusClientFactory;
import acromusashi.stream.config.StormConfigGenerator;
import acromusashi.stream.config.StormConfigUtil;
import acromusashi.stream.exception.InitFailException;
import backtype.storm.Config;
import backtype.storm.generated.ClusterSummary;
import backtype.storm.generated.SupervisorSummary;
import backtype.storm.utils.NimbusClient;
import ch.ethz.ssh2.Connection;
import ch.ethz.ssh2.SCPClient;
import ch.ethz.ssh2.SCPOutputStream;

/**
 * Config file put tool at storm cluster.<br>
 * This tool execute following procedure.<br>
 * <ol>
 * <li>Get storm supervisor nodes from nimbus.</li>
 * <li>Put specified config file to storm supervisor nodes by scp.</li>
 * </ol>
 * 
 * @author kimura
 */
public class ConfigPutTool
{
    /** Default config path */
    private static final String DEFAULT_CONFIG_PATH        = "/opt/storm/conf/storm.yaml";

    /** Default ssh port */
    private static final int    DEFAULT_SSH_PORT           = 22;

    /** Default nimbus thrift port */
    private static final int    DEFAULT_NIMBUS_THRIFT_PORT = 6627;

    /** Logger */
    private static final Logger logger                     = LoggerFactory.getLogger(ConfigPutTool.class);

    /** Path delimeter. The delimeter fix "/", becanse remote host os is only linux.  */
    private static final String PATH_DELIMETER             = "/";

    /**
     * Constructor
     */
    private ConfigPutTool()
    {
        // Do nothing.
    }

    /**
     * Program Entry Point<br>
     * <br>
     * Use following arguments.<br>
     * <ul>
     * <li>-c Config path(optional, default /opt/storm/conf/storm.yaml)</li>
     * <li>-s Put source path on local(required)</li>
     * <li>-d Put destination path on supervisors(required)</li>
     * <li>-ua Put user account(required)</li>
     * <li>-up Put user password(optional, default is not set)</li>
     * <li>-sp Ssh port(optional, default is 22)</li>
     * <li>-sh Show help</li>
     * </ul>
     * 
     * @param args Argments
     */
    public static void main(String... args)
    {
        ConfigPutTool putTool = new ConfigPutTool();
        putTool.startPutTool(args);
    }

    /**
     * Start config put tool.<br>
     * Check arguments and start tool.
     * 
     * @param args Argments
     */
    private void startPutTool(String[] args)
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
            help.printHelp(ConfigPutTool.class.getName(), cliOptions, true);
            return;
        }

        if (commandLine.hasOption("sh"))
        {
            // If help option setted, Show help and exit.
            help.printHelp(ConfigPutTool.class.getName(), cliOptions, true);
            return;
        }

        String configPath = DEFAULT_CONFIG_PATH;
        if (commandLine.hasOption("c") == true)
        {
            configPath = commandLine.getOptionValue("c");
        }

        String srcPath = commandLine.getOptionValue("s");
        String dstPath = commandLine.getOptionValue("d");

        String userAccount = commandLine.getOptionValue("ua");

        String userPassword = null;
        if (commandLine.hasOption("up") == true)
        {
            userPassword = commandLine.getOptionValue("up");
        }

        int sshPort = DEFAULT_SSH_PORT;
        if (commandLine.hasOption("sp") == true)
        {
            sshPort = Integer.parseInt(commandLine.getOptionValue("sp"));
        }

        try
        {
            executePutTool(configPath, srcPath, dstPath, userAccount, userPassword, sshPort);
        }
        catch (TException | UnknownHostException ex)
        {
            String logFormat = "Get target host failed. Exit tool.";
            logger.error(logFormat, ex);
        }
        catch (IOException ex)
        {
            String logFormat = "Config file put failed. Exit tool.";
            logger.error(logFormat, ex);
        }
    }

    /**
     * Execute config put tool.
     * 
     * @param configPath configPath
     * @param srcPath src Path
     * @param dstPath dst Path
     * @param userAccount user account
     * @param userPassword user password
     * @param sshPort Ssh port
     * @throws TException Get failed.
     * @throws IOException Put failed.
     */
    private void executePutTool(String configPath, String srcPath, String dstPath,
            String userAccount, String userPassword, int sshPort) throws TException, IOException
    {
        Config config = StormConfigGenerator.loadStormConfig(configPath);
        String nimbusHost = StormConfigUtil.getStringValue(config, Config.NIMBUS_HOST, "");
        int nimbusPort = StormConfigUtil.getIntValue(config, Config.NIMBUS_THRIFT_PORT,
                DEFAULT_NIMBUS_THRIFT_PORT);

        NimbusClientFactory factory = new NimbusClientFactory();
        NimbusClient client = factory.createClient(nimbusHost, nimbusPort);

        ClusterSummary clusterSummary = client.getClient().getClusterInfo();

        List<SupervisorSummary> supervisors = clusterSummary.get_supervisors();

        // Check Name > IPAddress convert check
        for (SupervisorSummary supervisor : supervisors)
        {
            InetAddress.getByName(supervisor.get_host());
        }

        String localHostName = InetAddress.getLocalHost().getHostName();

        // Put config file.
        // If target host is localhost and dstination path equals source path, no need to put and skip.
        for (SupervisorSummary supervisor : supervisors)
        {
            if (localHostName.equals(supervisor.get_host()) && srcPath.equals(dstPath))
            {
                continue;
            }

            putConfigToRemote(supervisor.get_host(), sshPort, srcPath, dstPath, userAccount,
                    userPassword);
        }
    }

    /**
     * Put config file each host.
     * 
     * @param targetHost Remote host
     * @param sshPort Ssh port
     * @param srcPath src Path
     * @param dstPath dst Path
     * @param userAccount user account
     * @param userPassword user password
     * @throws IOException 
     */
    private void putConfigToRemote(String targetHost, int sshPort, String srcPath, String dstPath,
            String userAccount, String userPassword) throws IOException
    {
        Connection connection = new Connection(targetHost, sshPort);
        try
        {
            connection.connect();
            boolean authenticated = false;
            if (StringUtils.isBlank(userPassword))
            {
                authenticated = connection.authenticateWithNone(userAccount);
            }
            else
            {
                authenticated = connection.authenticateWithPassword(userAccount, userPassword);
            }

            if (authenticated == false)
            {
                String errFormat = "Login failed. Exit tool. TargetHost={0}, User={1}";
                throw new InitFailException(
                        MessageFormat.format(errFormat, targetHost, userAccount));
            }

            putFile(connection, srcPath, dstPath);
        }
        finally
        {
            connection.close();
        }

    }

    /**
     * Put file to remote server.
     * 
     * @param connection Ssh Connection 
     * @param srcPath src Path
     * @param dstPath dst Path
     * @throws IOException Put failed
     */
    private void putFile(Connection connection, String srcPath, String dstPath) throws IOException
    {
        SCPClient client = new SCPClient(connection);

        File srcFile = new File(srcPath);
        String dstFileDir = StringUtils.substringBeforeLast(dstPath, PATH_DELIMETER);
        String dstFileName = StringUtils.substringAfterLast(dstPath, PATH_DELIMETER);

        try (BufferedInputStream in = new BufferedInputStream(new FileInputStream(srcFile));
                SCPOutputStream out = client.put(dstFileName, srcFile.length(), dstFileDir, "0664");)
        {
            IOUtils.copy(in, out);
        }
        catch (IOException ex)
        {
            throw ex;
        }
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

        // Source config path option
        OptionBuilder.hasArg(true);
        OptionBuilder.withArgName("Source config path");
        OptionBuilder.withDescription("Source config path");
        OptionBuilder.isRequired(true);
        Option sourceOption = OptionBuilder.create("s");

        // Destination config path option
        OptionBuilder.hasArg(true);
        OptionBuilder.withArgName("Destination config path");
        OptionBuilder.withDescription("Destination config path");
        OptionBuilder.isRequired(true);
        Option destinationOption = OptionBuilder.create("d");

        // User account option
        OptionBuilder.hasArg(true);
        OptionBuilder.withArgName("User account");
        OptionBuilder.withDescription("User account");
        OptionBuilder.isRequired(true);
        Option accountOption = OptionBuilder.create("ua");

        // User password option
        OptionBuilder.hasArg(true);
        OptionBuilder.withArgName("User password");
        OptionBuilder.withDescription("User password");
        OptionBuilder.isRequired(false);
        Option passwordOption = OptionBuilder.create("up");

        // Ssh port option
        OptionBuilder.hasArg(true);
        OptionBuilder.withArgName("Ssh port");
        OptionBuilder.withDescription("Ssh port");
        OptionBuilder.isRequired(false);
        Option sshPortOption = OptionBuilder.create("sp");

        // ヘルプオプション
        OptionBuilder.withDescription("show help");
        Option helpOption = OptionBuilder.create("sh");

        cliOptions.addOption(configOption);
        cliOptions.addOption(sourceOption);
        cliOptions.addOption(destinationOption);
        cliOptions.addOption(accountOption);
        cliOptions.addOption(passwordOption);
        cliOptions.addOption(sshPortOption);
        cliOptions.addOption(helpOption);
        return cliOptions;
    }
}
