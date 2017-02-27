package com.ryasale;

import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.catalina.Context;
import org.apache.catalina.LifecycleException;
import org.apache.catalina.startup.Tomcat;
import org.apache.spark.storage.StorageLevel;
import org.apache.spark.streaming.receiver.Receiver;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**
 * custom spark streaming receiver
 *
 * @author Alexander Ryasnyanskiy
 *         created on 20.02.17
 */
@Slf4j
public class HttpReceiver extends Receiver<String> {

    private int port;
    private String servletPattern;
    private Tomcat tomcat;

    public HttpReceiver(int port,
                        String servletPattern) {
        super(StorageLevel.MEMORY_ONLY());
        this.port = port;
        this.servletPattern = servletPattern;
    }


    public void onStart() {
        new Thread() {
            @Override
            public void run() {
                log.info("start receiver");
                startTomcat();
            }
        }.start();
    }

    public void onStop() {
        try {
            tomcat.stop();
        } catch (LifecycleException e) {
            log.error("stop tomcat error", e);
        }
    }

    /**
     * starts tomcat server
     */
    private void startTomcat() {

        String contextPath = "/";
        tomcat = new Tomcat();
        tomcat.setPort(port);
        Context ctx = tomcat.addContext(contextPath, new File(".").getAbsolutePath());

        ObjectMapper mapper = new ObjectMapper();

        Tomcat.addServlet(ctx, "EventServlet", new EventServlet(mapper));
        ctx.addServletMappingDecoded(servletPattern, "EventServlet");

        try {
            tomcat.start();
        } catch (LifecycleException e) {
            log.error("start tomcat error", e);
        }
        tomcat.getServer().await();

    }


    /**
     * receives http requests from tomcat and saves their params to spark receiver
     */
    @AllArgsConstructor
    private class EventServlet extends HttpServlet {

        private ObjectMapper mapper;

        @Override
        protected void doGet(HttpServletRequest req,
                             HttpServletResponse resp) throws ServletException, IOException {

            long timestamp = System.currentTimeMillis();

            Map<String, String[]> inParams = req.getParameterMap();

            Map<String, String[]> outParams = new HashMap<>();
            inParams.entrySet().forEach(e -> outParams.put(e.getKey(), e.getValue()));

            String[] timestampArr = {String.valueOf(timestamp)};
            outParams.put("timestamp", timestampArr);

            String outParamsJson = mapper.writeValueAsString(outParams);

            // store data from http request to the spark receiver
            store(outParamsJson);

        }

    }

}
