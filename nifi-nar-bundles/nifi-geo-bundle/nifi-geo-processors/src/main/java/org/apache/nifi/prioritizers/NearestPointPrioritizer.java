package org.apache.nifi.prioritizers;

import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.flowfile.FlowFilePrioritizer;

public class NearestPointPrioritizer implements FlowFilePrioritizer {
    private static final String LATITUDE = "latitude";
    private static final String LONGITUDE = "longitude";


    private double distance(double lat, double lon, double latO, double lonO){
        return Math.acos(
                Math.sin(lat) * Math.sin(latO)
                + Math.cos(lat) * Math.cos(latO) * Math.cos(lonO - lon)
            );

    }
    @Override
    public int compare(FlowFile o1, FlowFile o2) {
        // reference point
        // TODO - figure out a way of inserting the reference point
        // Current method is to take the most recent of the flow files and 
        double lat = 0;
        double lon = 0;
        
        double lat1 = Double.valueOf(o1.getAttribute(LATITUDE));
        double lon1 = Double.valueOf(o1.getAttribute(LONGITUDE));
        double lat2 = Double.valueOf(o2.getAttribute(LATITUDE));
        double lon2 = Double.valueOf(o2.getAttribute(LONGITUDE));
        
        double d1 = distance(lat, lon, lat1, lon1);
        double d2 = distance(lat, lon, lat2, lon2);
        
        return (int) Math.round(d1 - d2);         
    }

}
