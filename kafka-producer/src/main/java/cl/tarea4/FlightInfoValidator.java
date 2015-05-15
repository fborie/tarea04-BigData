package cl.tarea4;

import java.util.regex.Pattern;

/**
 * Created by cloudera on 5/14/15.
 */
public class FlightInfoValidator {

    private String[] infoParts;

    public FlightInfoValidator(){}

    public boolean isValidFlightInfo(String info){
        if(info.substring(0,1).equals("{") || info.substring(0,1).equals("}"))
            return false;
        else{
            setInfoParts(info);
            if(existsAircraftModel() && existsKey() && existsOrigin()
                    && existsDestiny())
                return true;
            return false;
        }
    }

    private void setInfoParts(String info){
        String patterString = "[^a-zA-Z0-9\".-]+";
        Pattern regexPattern = Pattern.compile(patterString);
        infoParts = regexPattern.split(info);
    }

    private boolean existsAircraftModel(){
        if(infoParts[10].equals("\"\"")){
            return false;
        }
        return true;
    }

    private boolean existsKey(){
        if(infoParts[11].equals("\"\"")){
            return false;
        }
        return true;
    }

    private boolean existsOrigin(){
        if(infoParts[13].equals("\"\"")){
            return false;
        }
        return true;
    }

    private boolean existsDestiny(){
        if(infoParts[14].equals("\"\""))
            return false;
        return true;
    }
}
