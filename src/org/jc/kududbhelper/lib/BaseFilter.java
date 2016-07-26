/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package org.jc.kududbhelper.lib;

import org.kududb.client.KuduPredicate;

/**
 * Class defining a tuple of the type <ColumnName, Operator, value|binary>
 * This is the class that will be used when filtering data inside a kudu table.
 * @author cespedjo
 */
public class BaseFilter {
    
    protected String columnName;
    
    protected KuduPredicate.ComparisonOp operator;
    
    protected Object value;
    
    protected byte[] bValue;

    public String getColumnName() {
        return columnName;
    }

    public void setColumnName(String columnName) {
        this.columnName = columnName;
    }

    public KuduPredicate.ComparisonOp getOperator() {
        return operator;
    }

    public void setOperator(KuduPredicate.ComparisonOp operator) {
        this.operator = operator;
    }

    public Object getValue() {
        return this.value;
    }
    
    public byte[] getBinaryValue() {
        return this.bValue;
    }

    public void setValue(Object value) {
        this.value = value;
    }
    
    public void setValue(byte[] value) {
        this.bValue = value;
    }
}
