/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package org.jc.kududbhelper.lib;

/**
 *
 * @author Jorge Cespedes
 */
public class KuduHelperFactory {
    
    private static KuduHelper helper = null;
    
    /**
     * Thread safe method to obtain an instance of KuduHelper class.
     * @param tableName name of the table where records are to be inserted.
     * @param masterAddresses an array of master addresses
     * @return an instance of KuduHelper
     * @throws Exception if any of the arguments is null.
     */
    public static synchronized KuduHelper buildKuduHelper(String tableName, String[] masterAddresses) 
            throws Exception {
        if (KuduHelperFactory.helper == null) {
            KuduHelperFactory.helper = new KuduHelper(tableName, masterAddresses);
        }
        
        return KuduHelperFactory.helper;
    }
}
