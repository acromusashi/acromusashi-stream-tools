import backtype.storm.utils.Utils;
import java.util.Map;

String result = Utils.readStormConfig().get(args[0]).toString();
System.out.println("VALUE: " + result);
