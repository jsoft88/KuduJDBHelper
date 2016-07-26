/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package org.jc.kududbhelper.lib;

import org.kududb.ColumnSchema;

/**
 *
 * @author cespedjo
 */
public interface Statement {
    
    void parseWhere(ColumnSchema columnSchema);
}
