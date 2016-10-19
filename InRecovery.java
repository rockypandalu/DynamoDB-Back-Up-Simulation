import java.util.HashMap;
import java.util.List;
import java.util.Map;

// Simulate the recovery process based using the back-up data (namely "current") and DynamoDB Streams (namely "logs")
public class InRecovery {
	List<Map<String, Object>> logs;
	Map<Integer, Integer> current;
	public InRecovery(Map<Integer, Integer> afterBackUp, List<Map<String, Object>> logs) {
		current = new HashMap<>(afterBackUp);
		this.logs = logs;
	}

//	When recovery we process each log in reverse time order
	public Map<Integer, Integer> start() {
		for (int i = logs.size() - 1; i >=0; i--) {
			controller(i);
		}
		return current;
	}
	
//	Based on the type of the log, decide which kind of action we should take
	private void controller(int index) {
		Map<String, Object> log = logs.get(index);
		String type = (String) log.get("type");
		switch (type) {
		case "Insert": insertRec(index);
						break;
		case "Remove": removeRec(index);
						break;
		case "Modify": modifyRec(index);
						break;
		default: System.out.println("Error: " + type);
						break;
		}
	}
	
//	Recovery the insert process, which is the same as remove (idempotent)
	private void insertRec(int index) {
		Map<String, Object> log = logs.get(index);
		int[] info = (int[]) log.get("new");
		int key = info[0];
		if (current.containsKey(key)) {
			current.remove(key);
		}
	}
	
//	Recovery the remove process, whic is the same as put item (idempotent)
	private void removeRec(int index) {
		Map<String, Object> log = logs.get(index);
		int[] info = (int[]) log.get("old");
		int key = info[0];
		int value = info[1];
		current.put(key, value);
	}
	
//	Recovery the modify process, which is the same as put item (idempotent)
	private void modifyRec(int index) {
		Map<String, Object> log = logs.get(index);
		int[] info = (int[]) log.get("old");
		int key = info[0];
		int value = info[1];
		current.put(key, value);
	}
}
