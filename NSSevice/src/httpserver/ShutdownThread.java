package httpserver;

import org.apache.log4j.Logger;
import org.eclipse.jetty.server.Server;

class ShutdownThread extends Thread {

    private final Server server;
    private static final Logger logger = Logger.getLogger(ShutdownThread.class);

    public ShutdownThread(Server server) {
        this.server = server;
    }

    @Override
    public void run() {
        logger.info("ZaloApiUM Server Waiting for shut down.........");
        try {
            server.stop();

        } catch (Exception ex) {
            logger.error(ex.getMessage());
        }

        logger.info("ZaloApiUM Server shutted down!");
    }

}
