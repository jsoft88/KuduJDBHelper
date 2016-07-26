/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package org.jc.kududbhelper.lib;

/**
 *
 * @author Jorge Cespedes
 * Every class representing a row in kudu must extend this class, it adds metadata
 * to each record.
 */
public abstract class BaseRow {
    
    private static final String[] EXCLUDE_FIELDS = {"dataAsJsonArray"};
    
    private String dataAsJsonArray;

    public String getDataAsJsonArray() {
        return dataAsJsonArray;
    }

    public void setDataAsJsonArray(String dataAsJsonArray) {
        this.dataAsJsonArray = dataAsJsonArray;
    }
    
    public static boolean isIgnoreField(String fieldName) {
        for (int i = 0; i < EXCLUDE_FIELDS.length; ++i) {
            if (EXCLUDE_FIELDS[i].equals(fieldName))
                return true;
        }
        
        return false;
    }
}
