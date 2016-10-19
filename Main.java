import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class Main {

	public static void main(String[] args) {
//		Generate 1 million of key-value pairs as original data before back-up
		Map<Integer, Integer> id_age = Generator.generator(1000000);
//		the list "logs" is used to simulate the information on DynamoDB Streams
		List<Map<String, Object>> logs = new ArrayList<>();
		
//		Simulate behavior during back-up
		InBackUp inBackUp = new InBackUp(id_age, logs);
		Map<Integer, Integer> afterBackUp = inBackUp.start();
		
//		Simulate behavior during revocery
		InRecovery inRecovery = new InRecovery(afterBackUp, logs);
		Map<Integer, Integer> afterRecovery = inRecovery.start();
		
//		Check whether the revocered data is the same as the data before back-up
		isSame(id_age, afterRecovery);
	}
	
	private static boolean isSame(Map<Integer, Integer> id_age, Map<Integer, Integer> afterRecovery) {
		if (id_age.size() != afterRecovery.size()) {
			System.out.println("The number of element changed");
			return false;
		}
		for (int key: id_age.keySet()) {
			if (!afterRecovery.containsKey(key)) {
				System.out.println("Exist different key");
				return false;
			}
			if (id_age.get(key) != afterRecovery.get(key)) {
				System.out.println("Exist different value");
				return false;
			}
		}
		System.out.println("Recovery Succeed");
		return true;
	}


}
