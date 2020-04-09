package com.testing.mdc;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.crunchy.mdc.common.Context;
import org.crunchy.mdc.tasks.executor.Combiner;
import org.crunchy.mdc.tasks.executor.CrunchCombiner;
import org.crunchy.mdc.tasks.executor.CrunchMapper;
import org.crunchy.mdc.tasks.executor.CrunchReducer;
import org.crunchy.mdc.tasks.executor.Mapper;
import org.crunchy.mdc.tasks.executor.Reducer;

public class CarriersDataMapper implements CrunchMapper<Long,String,Context<String,Map>>, CrunchReducer<String,Map,Context<String,String>>,
CrunchCombiner<String,Map,Context<String,Map>>
{

	@Override
	public void map(Long chunkid, String line, Context<String, Map> ctx) {
		String[] contents = line.split(",");
		if(contents[0]!=null && !contents[0].equals("Code")) {
			if(contents!=null && contents.length>1 && contents[1]!=null && !contents[1].equals("NA")) {
				Map<String,String> map = new HashMap<>();
				map.put("CARRIERS", contents[1].substring(1,contents[1].length()-1));
				ctx.put(contents[0].substring(1,contents[0].length()-1), map);
			}
		}
	}

	@Override
	public void reduce(String key, List<Map> values, Context<String, String> context) {
		long sum=0;
	   	 String carrierName = "";
	   	 for(Map map:values) {
	   		 if(map.get("CARRIERS")!=null) {
	   			 carrierName= (String) map.get("CARRIERS"); 
	   		 }
	   		 else if(map.get("AIRLINE")!=null) {
	   			 sum+=(long)map.get("AIRLINE");
	   		 }
	   	 }
	   	 if(sum>0) {
	   		 context.put(key,  carrierName.toString()+","+sum+"");
	   	 }
		
	}

	@Override
	public void combine(String key, List<Map> values, Context<String, Map> context) {
		long sum=0;
	   	 String carrierName = "";
	   	 for(Map map:values) {
	   		 if(map.get("CARRIERS")!=null) {
	   			context.put(key,  map);
	   		 }
	   		 else if(map.get("AIRLINE")!=null) {
	   			 sum+=Long.parseLong((String) map.get("AIRLINE"));;
	   		 }
	   	 }
	   	 if(sum!=0) {
	   		 System.out.println(sum);
	   		 Map map1 = new HashMap<>();
	   		 map1.put("AIRLINE", sum);
	   		 context.put(key,  map1);
	   	 }
		
	}

}
