import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class InBackUp {
	List<Map<String, Object>> logs;
	Map<Integer, Integer> current;
	public InBackUp(Map<Integer, Integer> id_age, List<Map<String, Object>> logs) {
		current = new HashMap<>(id_age);
		this.logs = logs;
	}
	
//	Simulate how data backed-up in S3 (namely "currently") and DynamoDB Streams (namely "logs") influenced by new coming data during back-up
	public Map<Integer, Integer> start() {
		put(1, 20000, 5);
		put(1, 20000, 6);
		remove(2, 20000);
		put(3, 50, 3);
		remove(3, 9);
		for (int t = 10; t < 1000; t++) {
			for (int operation = 0; operation < 100; operation++) {
				put(t, (int)(Math.random() * 20000000), (int) (Math.random()*100));
				remove(t, (int)(Math.random() * 2000000));
			}
		}
		return current;
	}
	
//	Randomly decide whether one change during the back-up will be stored in the back-up table
	private boolean isBackUp () {
		return Math.random() > 0.5;
	}

//	DynamoDB allows client to delete items remotly, and will generate "Remove" Log in the DynamoDB Streams
//	The remove function is used to simulate this delete process
	private void remove(int t, int key) {
//		NOTE: In DynamoDB, when client removing a key that is not exist in DynamoDB, no log will be generated in DynamoDB Streams
		if (!current.containsKey(key)) {
			return;
		}
		int value = current.get(key);
		if (isBackUp()) {
			current.remove(key);
		}
		Map<String, Object> log = new HashMap<>();
		log.put("type", "Remove");
		log.put("time", t);
		log.put("old", new int[]{key, value});
		logs.add(log);
	}
	
//	When client communicating with DynamoDB, they can "put" item in DynamoDB
//	This action will cause two different log formats (insert and modify) depends on whether the item's key is already in the DynamoDB
	private void put(int t, int key, int value) {
		if (!current.containsKey(key)) {
			insert(t, key, value);
		} else {
			modify(t, key, value);
		}
	}
	
	private void insert(int t, int key, int value) {
		if (isBackUp()) {
			current.put(key, value);
		}
		Map<String, Object> log = new HashMap<>();
		log.put("type", "Insert");
		log.put("time", t);
		log.put("new", new int[]{key, value});
		logs.add(log);
	}
	

	private void modify(int t, int key, int newValue) {
		int oldValue = current.get(key);
//		NOTE: In DynamoDB, when client updates an item with same key and value as the one in the DynamoDB table, no log will be generated in DynamoDB Streams
		if (oldValue == newValue) {
			return;
		}
		if (isBackUp()) {
			current.put(key, newValue);
		}
		Map<String, Object> log = new HashMap<>();
		log.put("type", "Modify");
		log.put("time", t);
		log.put("old", new int[]{key, oldValue});
		log.put("new", new int[]{key, newValue});
		logs.add(log);
	}
}
