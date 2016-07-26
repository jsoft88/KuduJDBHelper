/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package org.jc.kududbhelper.lib;

import java.util.logging.Level;
import java.util.logging.Logger;
import org.kududb.client.SessionConfiguration;

/**
 *
 * @author cespedjo
 */
public class KuduHelperMain {
    
    private static final String SYNC_MODE = "sync";
    
    private static final String ASYNC_MODE = "async";
    
    private static final String MASTERS_DELIMITED = ",";
    
    public static void main(String[] args) {
        if (args == null || args.length < 4) {
            System.out.println("Usage: KuduHelperMain <tableName> <sync | async> <jsonArray> <master1:port,master2:port...masternumMaster:port>");
            System.out.println("The program expects the name of the table, the type of session");
            System.out.println("to be created: sync or async session and data expressed as a json array.");
            System.out.println("Keys in a given json must match columns name.");
            System.out.println("Also include a list of master addresses separated by ',', with no spaces.");
            
            return;
        }
        
        String tableName = args[0];
        String sessionMode = args[1];
        String jsonArray = args[2];
        String commaMasters = args[3];
        
        try {
            KuduHelper helper =
                    KuduHelperFactory.buildKuduHelper(tableName, commaMasters.split(MASTERS_DELIMITED));
            helper.init(sessionMode.equalsIgnoreCase(SYNC_MODE) ? 
                    KuduHelper.KUDU_SESSION_SYNC : KuduHelper.KUDU_SESSION_ASYNC, 
                    SessionConfiguration.FlushMode.AUTO_FLUSH_BACKGROUND);
            
            helper.processJson(jsonArray);
            
            System.out.println("\n\nNo exceptions occurred. Check your table to see newly inserted records...");
        } catch (Exception ex) {
            Logger.getLogger(KuduHelperMain.class.getName()).log(Level.SEVERE, null, ex);
        }
    }
}
