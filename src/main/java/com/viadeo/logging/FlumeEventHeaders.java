package com.viadeo.logging;

/**
 * Description :
 * Date: 08/03/13 at 15:42
 *
 * @author <a href="mailto:cnguyen@viadeoteam.com">Christian NGUYEN VAN THAN</a>
 */
public enum FlumeEventHeaders {

    TYPE("flume.client.log4j.type"),
    FORMAT("flume.client.log4j.format"),
    VERSION("flume.client.log4j.version");

    private String headerName;

    private FlumeEventHeaders(String headerName){
        this.headerName = headerName;
    }

    public String getName(){
        return headerName;
    }

    public String toString(){
        return getName();
    }

}
