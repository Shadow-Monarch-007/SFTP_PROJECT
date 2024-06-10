package com.servo.businessdoc;

//import com.servo.esaf.casa.input.*;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.net.HttpURLConnection;
import java.net.URL;
import java.net.URLConnection;
import java.security.cert.X509Certificate;
import java.util.HashMap;
import java.util.Map;
import javax.net.ssl.HostnameVerifier;
import javax.net.ssl.HttpsURLConnection;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLSession;
import javax.net.ssl.TrustManager;
import javax.net.ssl.X509TrustManager;

public class AdharVaultConnect {

    public Map<String, Object> connect(String urlPrf, String inputXml) {
        Map<String, Object> response = new HashMap<String, Object>();
        StringBuilder result = new StringBuilder();
        try {

            // Create a trust manager that does not validate certificate chains
            TrustManager[] trustAllCerts = new TrustManager[]{new X509TrustManager() {
                public java.security.cert.X509Certificate[] getAcceptedIssuers() {
                    return null;
                }

                public void checkClientTrusted(X509Certificate[] certs, String authType) {
                }

                public void checkServerTrusted(X509Certificate[] certs, String authType) {
                }
            }
            };

            // Install the all-trusting trust manager
            SSLContext sc = SSLContext.getInstance("SSL");
            sc.init(null, trustAllCerts, new java.security.SecureRandom());
            HttpsURLConnection.setDefaultSSLSocketFactory(sc.getSocketFactory());

            // Create all-trusting host name verifier
            HostnameVerifier allHostsValid = new HostnameVerifier() {
                public boolean verify(String hostname, SSLSession session) {
                    return true;
                }
            };

            // Install the all-trusting host verifier
            HttpsURLConnection.setDefaultHostnameVerifier(allHostsValid);

            URL url = new URL(urlPrf);
//            URLConnection con = url.openConnection();
//            HttpsURLConnection con = (HttpsURLConnection) url.openConnection();
            HttpURLConnection con = (HttpURLConnection) url.openConnection();
            System.out.println("URL-- " + url);
            // specify that we will send output and accept input
            con.setDoInput(true);
            con.setDoOutput(true);
            con.setRequestMethod("POST");
            con.setConnectTimeout(50000);  // long timeout, but not infinite
            con.setReadTimeout(50000);
            con.setUseCaches(false);
            con.setDefaultUseCaches(false);
            // tell the web server what we are sending
//            con.setRequestProperty("Content-Type", "application/xml");
            con.setRequestProperty("Content-Type", "application/json; charset=utf-8; protocol=gRPC");
            //con.setRequestProperty("channelId", "SERVO");
            con.setRequestProperty("Authorization", "");
            con.setRequestProperty("X-Request-ID", "");
            OutputStreamWriter writer = new OutputStreamWriter(con.getOutputStream());
            writer.write(inputXml);
            writer.flush();
            writer.close();

            // reading the response
            InputStreamReader reader = new InputStreamReader(con.getInputStream());
            StringBuilder buf = new StringBuilder();
            char[] cbuf = new char[2048];
            int num;
            while (-1 != (num = reader.read(cbuf))) {
                buf.append(cbuf, 0, num);
            }
            result = buf;
            response.put("status", 1);
            response.put("data", result);
            System.out.println("AdharValut after POST:" + result);

        } catch (Throwable t) {
            response.put("status", 0);
            response.put("msg", t.getLocalizedMessage());
            System.err.println(" AdharValut after POST: Exception --" + t);
            t.printStackTrace();
        }
        return response;
    }
}
