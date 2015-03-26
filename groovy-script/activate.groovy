import backtype.storm.utils.NimbusClient;
import backtype.storm.utils.Utils;
import java.util.Map;

Map config = Utils.readStormConfig();
NimbusClient nimbusClient = NimbusClient.getConfiguredClient(config);
nimbusClient.getClient().activate(args[0]);

System.out.println("Activated topology: " + args[0]);