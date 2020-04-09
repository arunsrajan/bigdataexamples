package com.testing.mdc;

import org.crunchy.mdc.tasks.scheduler.MdcJob;
import org.crunchy.mdc.tasks.scheduler.MdcJobBuilder;

public class MrJobTest {

	public static void main(String[] args) {
		MdcJob mdcjob = MdcJobBuilder.newBuilder()
				.addMapper(AirlineDataMapper.class, "/airlinecomplete")
				.addMapper(AirlineDataMapper.class, "/airlinecomplete2")
				.addMapper(CarriersDataMapper.class, "/carriers")
				.addCombiner(CarriersDataMapper.class)
				.addReducer(CarriersDataMapper.class)
				.setOutputfolder("/newmapperout")
				.build();
				
		mdcjob.run();
	}

}
