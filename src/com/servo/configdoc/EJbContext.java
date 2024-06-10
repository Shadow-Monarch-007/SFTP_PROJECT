package com.servo.configdoc;

import java.util.Properties;
import javax.naming.Context;
import javax.naming.InitialContext;

public class EJbContext {

    public static Object getContext(String ejbName, String module, String ip, String port) {

        try {
            Properties jndiProperties = new Properties();
            jndiProperties.put(Context.URL_PKG_PREFIXES, "org.jboss.ejb.client.naming");
            jndiProperties.put(Context.INITIAL_CONTEXT_FACTORY, "org.jboss.naming.remote.client.InitialContextFactory");
            jndiProperties.put("java.naming.provider.url", "remote://" + ip + ":" + port);
            jndiProperties.put("jboss.naming.client.ejb.context", true);

            Context ctx = null;
            try {
                ctx = new InitialContext(jndiProperties);
            } catch (Exception ex) {
                System.out.println("inside exception of JNDI InitialContext");
                ex.printStackTrace();
            }
            String Module = "ejb:/" + module + "//";
            ejbName = ejbName + "!" + "com.servo.dms.module.remote.DocumentRemoteModule";
            Module = Module + ejbName;
            System.out.println("Module : " + Module);
            return ctx.lookup(Module);

        } catch (Exception ex) {
            return ex.getMessage();
        }

    }

}
