package com.viadeo.logging;

import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Description :
 * Date: 06/03/13 at 12:14
 *
 * @author <a href="mailto:cnguyen@viadeoteam.com">Christian NGUYEN VAN THAN</a>
 */
public class FlumeAppenderTest {

    Logger log = LoggerFactory.getLogger(FlumeAppender.class);


    @Test
    public void testAppender() {
        try{
            Integer.parseInt("toto");
        } catch(NumberFormatException e) {
            log.error("test exception",e);
        }
        log.info("test");
    }

    @Test
    public void testBadAppenderUse() {
        log.info(null);
    }


}
