import backtype.storm.generated.ClusterSummary;
import backtype.storm.generated.TopologySummary;
import backtype.storm.utils.NimbusClient;
import backtype.storm.utils.Utils;
import java.util.Map;

Map config = Utils.readStormConfig();
NimbusClient nimbusClient = NimbusClient.getConfiguredClient(config);
ClusterSummary clusterSummary = nimbusClient.getClient().getClusterInfo();

if (clusterSummary.get_topologies().isEmpty())
{
    System.out.println("No topologies running.");
    return;
}

System.out.println("");
String topologyPattern = "%-20s %-10s %-10s %-12s %-10s";
System.out.println(String.format(topologyPattern, "Topology_name", "Status",
        "Num_tasks", "Num_workers", "Uptime_secs"));
System.out.println("-------------------------------------------------------------------");
for (TopologySummary topologySummary : clusterSummary.get_topologies())
{
    String topologyName = topologySummary.get_name();
    String status = topologySummary.get_status();
    int numTasks = topologySummary.get_num_tasks();
    int numWorkers = topologySummary.get_num_workers();
    int uptimeSecs = topologySummary.get_uptime_secs();
    System.out.println(String.format(topologyPattern, topologyName, status,
            numTasks, numWorkers, uptimeSecs));
}

