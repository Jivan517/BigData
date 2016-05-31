package cs522.utils;

import java.util.List;
import java.util.Map;

public class Printer<P, Q, R> {

	public static <P, Q, R> void printListOfKeyValueMapPair(List<KeyValuePair<P, Map<Q, R>>> list) {
		if (list != null) {
			for (KeyValuePair<P, Map<Q, R>> dict : list) {
				System.out.println("<" + dict.getKey() + "," + dict.getValue() + ">");

			}
		}
	}

	public static <P, Q, R> void printListOfGroupByMapPair(List<GroupByPair<P, Map<Q, R>>> list) {
		if (list != null) {
			for (GroupByPair<P, Map<Q, R>> item : list) {
				System.out.println("<" + item.getKey() + "," + item.getValues() + ">");

			}
		}
	}

	public static <P, Q, R> void printListOfKeyValuePairs(List<KeyValuePair<KeyValuePair<P, Q>, R>> list) {
		if (list != null) {
			for (KeyValuePair<KeyValuePair<P, Q>, R> dict : list) {
				System.out.println("<" + dict.getKey() + "," + dict.getValue() + ">");

			}
		}
	}

	public static <P, Q> void printListOfKeyValuePair(List<KeyValuePair<P, Q>> list) {
		if (list != null) {
			for (KeyValuePair<P, Q> dict : list) {
				System.out.println("<" + dict.getKey() + "," + dict.getValue() + ">");

			}
		}
	}

	public static <P, Q, R> void printListOfGroupByPair(List<GroupByPair<KeyValuePair<P, Q>, R>> list) {
		if (list != null) {
			for (GroupByPair<KeyValuePair<P, Q>, R> item : list) {
				System.out.println("<" + item.getKey() + "," + item.getValues() + ">");

			}
		}
	}

}
