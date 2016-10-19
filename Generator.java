import java.util.HashMap;
import java.util.Map;

//Generate key-value pairs as original data before back-up
public class Generator {
	public static Map<Integer, Integer> generator(int n) {
		Map<Integer, Integer> id_age = new HashMap<>();
		for (int i = 0; i < n; i++) {
			id_age.put(i, i % 100);
		}
		return id_age;
	}
}
