package com.servo.businessdoc;

public class XmlParser {

    private String parseString;
    private String copyString;
    private int IndexOfPrevSrch;

    public XmlParser() {
    }

    public void setInputXML(String ParseThisString) {
        if (ParseThisString != null) {
            copyString = new String(ParseThisString);
            parseString = toUpperCase(copyString, 0, 0);
            IndexOfPrevSrch = 0;
        } else {
            parseString = null;
            copyString = null;
            IndexOfPrevSrch = 0;
        }
    }

    public String getValueOf(String valueOf) {
        try {
            return new String(copyString.substring(parseString.indexOf("<"
                    + toUpperCase(valueOf, 0, 0) + ">") + valueOf.length() + 2,
                    parseString.
                    indexOf("</"
                            + toUpperCase(valueOf, 0, 0) + ">")));
        } catch (StringIndexOutOfBoundsException stringindexoutofboundsexception) {
            return "";
        }
    }

    public String toUpperCase(String valueOf, int begin, int end) throws
            StringIndexOutOfBoundsException {
        String returnStr = "";
        try {
            int count = valueOf.length();
            char strChar[] = new char[count];
            valueOf.getChars(0, count, strChar, 0);
            while (count-- > 0) {
                strChar[count] = Character.toUpperCase(strChar[count]);
            }
            returnStr = new String(strChar);
        } catch (ArrayIndexOutOfBoundsException arrayindexoutofboundsexception) {
        }
        return returnStr;
    }

}
