import backtype.storm.utils.NimbusClient;
import backtype.storm.utils.Utils;
import java.util.Map;

Map config = Utils.readStormConfig();
NimbusClient nimbusClient = NimbusClient.getConfiguredClient(config);
nimbusClient.getClient().deactivate(args[0]);

System.out.println("Deactivated topology: " + args[0]);
