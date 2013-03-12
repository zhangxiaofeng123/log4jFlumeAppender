package com.viadeo.logging;

import org.apache.flume.Event;
import org.apache.flume.EventDeliveryException;
import org.apache.flume.FlumeException;
import org.apache.flume.api.RpcClient;
import org.apache.flume.api.RpcClientFactory;
import org.apache.flume.clients.log4jappender.Log4jAvroHeaders;
import org.apache.flume.event.EventBuilder;
import org.apache.log4j.AppenderSkeleton;
import org.apache.log4j.helpers.LogLog;
import org.apache.log4j.spi.LoggingEvent;

import java.nio.charset.Charset;
import java.util.HashMap;
import java.util.Map;

/**
 * Description : Inspired by Log4jAppender
 * Date: 06/03/13 at 10:17
 *
 * @author <a href="mailto:cnguyen@viadeoteam.com">Christian NGUYEN VAN THAN</a>
 */
public class FlumeAppender extends AppenderSkeleton {

    public static final String EMPTY = "";

    private String hostname;
    private int port;
    private String type = EMPTY;
    private String format = EMPTY;
    private String version = EMPTY;

    private RpcClient rpcClient = null;
    private boolean isConfigured = false;

    private static final String CONNECT_FAILED =    "[Flume] Client configuration failed !! Connection failed on host:%s, port:%s\n" +
                                                    "Check your log4j.properties file\n" +
                                                    "log4j.appender.flume=com.viadeo.logging.FlumeAppender\n" +
                                                    "log4j.appender.flume.port=41414\n" +
                                                    "log4j.appender.flume.hostname=0.0.0.0\n" +
                                                    "log4j.appender.flume.name=yourModule\n" +
                                                    "log4j.appender.flume.type=error\n" +
                                                    "log4j.appender.flume.format=text\n" +
                                                    "log4j.appender.flume.version=1";

    /**
     * If this constructor is used programmatically rather than from a log4j conf
     * you must set the <tt>port</tt> and <tt>hostname</tt> and then call
     * <tt>activateOptions()</tt> before calling <tt>append()</tt>.
     */
    public FlumeAppender() {
    }

    /**
     * Sets the hostname and port. Even if these are passed the
     * <tt>activateOptions()</tt> function must be called before calling
     * <tt>append()</tt>, else <tt>append()</tt> will throw an Exception.
     *
     * @param hostname The first hop where the client should connect to.
     * @param port     The port to connect on the host.
     * @param type     The event type (ex: error)
     * @param format   The event format (ex: json)
     * @param version  The event version
     */
    public FlumeAppender(String hostname, int port, String type, String format, String version) {
        this.hostname = hostname;
        this.port = port;
        this.type = type;
        this.format = format;
        this.version = version;
    }

    /**
     * Activate the options set using <tt>setPort()</tt>
     * and <tt>setHostname()</tt>
     *
     * @throws FlumeException if the <tt>hostname</tt> and
     *                        <tt>port</tt> combination is invalid.
     */
    @Override
    public void activateOptions() throws FlumeException {
        try {
            rpcClient = RpcClientFactory.getDefaultInstance(hostname, port);
            isConfigured = true;
        } catch (FlumeException e) {
            String errormsg = "RPC client creation failed! --> " + e.getMessage();
            LogLog.error(errormsg);
        }
    }

    /**
     * Make it easy to reconnect on failure
     *
     * @throws FlumeException
     */
    private void reconnect() throws FlumeException {
        close();
        activateOptions();
    }

    /**
     * Append the LoggingEvent, to send to the first Flume hop.
     *
     * @param event The LoggingEvent to be appended to the flume.
     * @throws FlumeException if the appender was closed,
     *                        or the hostname and port were not setup, there was a timeout, or there
     *                        was a connection error.
     */
    @Override
    public synchronized void append(LoggingEvent event) {

        // If rpcClient is null, it means either that is was not properly configured
        // or the flume client is unreachable (example in development environment).
        // If the client is null, we silently fail and do nothing more.

        // get out fast when no message to log
        if (event.getMessage() == null) {
            return;
        }

        // if appender is not properly set up, just do nothing
        if ( ! isConfigured) {
            return;
        }

        // if client has disconnect, try to reconnect
        if (!rpcClient.isActive()) {
            reconnect();
        }

        // set event header section
        Map<String, String> headers = buildEventHeaders(event);

        // build the event body
        String eventBody = buildEventBody(event);


        // create flume event to send
        Event flumeEvent = EventBuilder.withBody(eventBody, Charset.forName("UTF8"), headers);

        try {
            rpcClient.append(flumeEvent);
        } catch (EventDeliveryException e) {
            String msg = "Flume append() failed.";
            LogLog.error(msg);
            throw new FlumeException(msg + " Exception follows.", e);
        }
    }

    /**
     * Construct the event headers base on event information and appender configuration
     * @param event the loggingEvent
     * @return a Map containing the event headers
     */
    private Map<String, String> buildEventHeaders(LoggingEvent event) {
        Map<String, String> hdrs = new HashMap<String, String>();
        hdrs.put(Log4jAvroHeaders.LOGGER_NAME.toString(), event.getLoggerName());
        hdrs.put(Log4jAvroHeaders.TIMESTAMP.toString(), String.valueOf(event.getTimeStamp()));
        hdrs.put(Log4jAvroHeaders.LOG_LEVEL.toString(), String.valueOf(event.getLevel().toInt()));
        hdrs.put(Log4jAvroHeaders.MESSAGE_ENCODING.toString(), "UTF8");
        hdrs.put(FlumeEventHeaders.FORMAT.toString(), format);
        hdrs.put(FlumeEventHeaders.TYPE.toString(), type);
        hdrs.put(FlumeEventHeaders.VERSION.toString(), version);
        return hdrs;
    }

    /**
     * Construct event body base on event informations
     * @param event the loggingEvent
     * @return a String representing the event body
     */
    private String buildEventBody(LoggingEvent event) {

        // create event body
        String[] exceptionMsgs = event.getThrowableStrRep();
        StringBuilder eventMsg = new StringBuilder(event.getMessage().toString());
        if (exceptionMsgs != null) {
            eventMsg.append("\n");
            for(String error : exceptionMsgs) {
                eventMsg.append(error).append("\n");
            }
        }
        return eventMsg.toString();
    }

    //This function should be synchronized to make sure one thread
    //does not close an appender another thread is using, and hence risking
    //a null pointer exception.

    /**
     * Closes underlying client.
     * If <tt>append()</tt> is called after this function is called,
     * it will throw an exception.
     *
     * @throws FlumeException if errors occur during close
     */
    @Override
    public synchronized void close() throws FlumeException {
        //Any append calls after this will result in an Exception.
        if (rpcClient != null) {
            rpcClient.close();
            rpcClient = null;
        }
    }

    @Override
    public boolean requiresLayout() {
        return false;
    }

    /**
     * Set the first flume hop hostname.
     *
     * @param hostname The first hop where the client should connect to.
     */
    public void setHostname(String hostname) {
        this.hostname = hostname;
    }

    /**
     * Set the port on the hostname to connect to.
     *
     * @param port The port to connect on the host.
     */
    public void setPort(int port) {
        this.port = port;
    }

    /**
     * Set the event type
     *
     * @param type
     */
    public void setType(String type) {
        this.type = type;
    }

    /**
     * Set the event format
     *
     * @param format
     */
    public void setFormat(String format) {
        this.format = format;
    }

    /**
     * Set the event version
     *
     * @param version
     */
    public void setVersion(String version) {
        this.version = version;
    }

}
