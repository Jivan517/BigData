package cs522.lab2;

import java.util.*;

import cs522.lab2.utils.GroupByPair;
import cs522.lab2.utils.KeyValuePair;

public class WordCountReducer {

	public List<GroupByPair<String, Integer>> combine(List<KeyValuePair<String, Integer>> list){
		
		List<GroupByPair<String, Integer>> groupByPairs = new ArrayList<GroupByPair<String, Integer>>();
		if(list != null){
			
			String prevKey = "";
			GroupByPair<String, Integer> groupPair = new GroupByPair<String, Integer>();
			for(KeyValuePair<String, Integer> keyVal : list){
				
				String key = keyVal.getKey();
				int val = keyVal.getValue();
				
				if(prevKey.equals(key)){
					List<Integer> values = groupPair.getValues();
					System.out.println(values);
					List<Integer> listValues = new ArrayList<>(values);
					listValues.add(val);
					groupPair.setValues(listValues);
				}
				
				else{
					
					if(groupPair.getKey() != null)
						groupByPairs.add(groupPair);
					groupPair = new GroupByPair<>();
					groupPair.setKey(key);
					groupPair.setValues(Arrays.asList(val));
				}
				
				prevKey = key;
			}
			if(groupPair.getKey() != null)
				groupByPairs.add(groupPair);
			
			return groupByPairs;
		}
		
		return null;
	}
	
	public KeyValuePair<String, Integer> reduce(GroupByPair<String, Integer> groupByPair){
		
		KeyValuePair<String, Integer> keyVal = new KeyValuePair<>();
		if(groupByPair != null){
			String key = groupByPair.getKey();
			int sum = 0;
			for(Integer val : groupByPair.getValues()){
				sum += val;
			}
			
			return new KeyValuePair<>(key, sum);
		}
		
		return null;
	}
	
	public List<KeyValuePair<String, Integer>> wordCountReduce(List<GroupByPair<String, Integer>> pairs){
		
		List<KeyValuePair<String, Integer>> reducedList = new ArrayList<>();
		if(pairs != null){
		
			for(GroupByPair<String, Integer> pair : pairs){
				
				reducedList.add(reduce(pair));
			}
			
			return reducedList;
		}
		
		return null;
	}
	
	
	
}
