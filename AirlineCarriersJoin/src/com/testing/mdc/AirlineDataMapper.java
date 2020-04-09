package com.testing.mdc;

import java.util.HashMap;
import java.util.Map;

import org.crunchy.mdc.common.Context;
import org.crunchy.mdc.tasks.executor.CrunchMapper;
import org.crunchy.mdc.tasks.executor.Mapper;

public class AirlineDataMapper implements CrunchMapper<Long, String, Context<String, Map>> {

	@Override
	public void map(Long chunkid, String line, Context<String, Map> ctx) {
		String[] contents = line.split(",");
		Map<String,String> map = new HashMap<>();
		if(contents[0]!=null && !contents[0].equals("Year")) {
			if(contents!=null && contents.length>14 && contents[14]!=null && !contents[14].equals("NA")) {
			map.put("AIRLINE", contents[14]);
			ctx.put(contents[8], map);
			}
		}
	}

}
