package cl.tarea4;

import java.util.regex.Pattern;

/**
 * Created by cloudera on 5/13/15.
 */
public class FlightInfoJsonParser {

    public static final String TAG = "FlightInfoParser";

    private String[] flightInfoParts;

    public FlightInfoJsonParser(){}

    public void setFlightInfo(String info){
        String patterString = "[^a-zA-Z0-9\".-]+";
        Pattern regexPattern = Pattern.compile(patterString);
        flightInfoParts = regexPattern.split(info);
    }

    public String getKey(){
        int len = flightInfoParts[11].length();
        return flightInfoParts[11].substring(1,len - 1);
    }

    public String getLatitude(){
        return flightInfoParts[3];
    }

    public String getLongitude(){
        return flightInfoParts[4];
    }

    public String getAltitude(){
        return flightInfoParts[6];
    }

    public String getSpeed(){
        return flightInfoParts[7];
    }

    public String getAircraftModel(){
        int len = flightInfoParts[10].length();
        return flightInfoParts[10].substring(1, len - 1);
    }

    public String getOrigin(){
        int len = flightInfoParts[13].length();
        return flightInfoParts[13].substring(1, len - 1);
    }

    public String getDestiny(){
        int len = flightInfoParts[14].length();
        return flightInfoParts[14].substring(1, len - 1);
    }

}
