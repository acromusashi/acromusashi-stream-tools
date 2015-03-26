import backtype.storm.generated.KillOptions;
import backtype.storm.utils.NimbusClient;
import backtype.storm.utils.Utils;
import java.util.Map;

Map config = Utils.readStormConfig();
NimbusClient nimbusClient = NimbusClient.getConfiguredClient(config);

int waitSecs = -1;
if(args.length >= 3 && ("-w".equals(args[1]) || ("--wait".equals(args[1]))))
{
    waitSecs = Integer.valueOf(args[2]);
}

if(waitSecs > 0)
{
    KillOptions options = new KillOptions();
    options.set_wait_secs(waitSecs);
    nimbusClient.getClient().killTopologyWithOpts(args[0], options);
}
else
{
    nimbusClient.getClient().killTopology(args[0]);
}

System.out.println("Killed topology: " + args[0]);
