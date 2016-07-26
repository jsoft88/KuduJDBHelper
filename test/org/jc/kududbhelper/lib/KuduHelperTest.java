/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package org.jc.kududbhelper.lib;

import com.google.gson.Gson;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.kududb.Type;
import org.kududb.client.KuduPredicate;
import org.kududb.client.RowResult;
import org.kududb.client.SessionConfiguration;

/**
 *
 * @author cespedjo
 */
public class KuduHelperTest {
    
    @Rule public ExpectedException thrown = ExpectedException.none();
    
    public KuduHelperTest() {
    }
    
    @BeforeClass
    public static void setUpClass() {
    }
    
    @AfterClass
    public static void tearDownClass() {
    }
    
    @Before
    public void setUp() {
    }
    
    @After
    public void tearDown() {
    }

    /**
     * Test of init method, of class KuduHelper.
     * @throws java.lang.Exception
     */
    @Test
    public void testInit() throws Exception{
        System.out.println("init");
        short sessionType = 0;
        SessionConfiguration.FlushMode flushMode = null;
        
        thrown.expect(Exception.class);
        thrown.expectMessage("Kudu Table or Master address cannot be null nor empty.");
        KuduHelper instance = KuduHelperFactory.buildKuduHelper("", null);
        instance.init(sessionType, flushMode);
        
    }
    
    @Test
    public void testQueryData_List() throws Exception {
        KuduHelper instance = KuduHelperFactory
                .buildKuduHelper("person", new String[]{"cdd7.sis.personal.net.py:7051"});
        instance.init(KuduHelper.KUDU_SESSION_ASYNC, SessionConfiguration.FlushMode.AUTO_FLUSH_BACKGROUND);
        List<BaseFilter> filters = new ArrayList<>();
        BaseFilter filter1 = new BaseFilter();
        filter1.setColumnName("id");
        filter1.setOperator(KuduPredicate.ComparisonOp.LESS);
        filter1.setValue(50);
        
        filters.add(filter1);
        
        BaseFilter filter2 = new BaseFilter();
        filter2.setColumnName("age");
        filter2.setOperator(KuduPredicate.ComparisonOp.GREATER_EQUAL);
        filter2.setValue(30);
        
        filters.add(filter2);
        
        BaseFilter filter3 = new BaseFilter();
        filter3.setColumnName("age");
        filter3.setOperator(KuduPredicate.ComparisonOp.LESS_EQUAL);
        filter3.setValue(40);
        
        filters.add(filter3);
        
        List<RowResult> results = instance.queryData(filters);
        Assert.assertEquals(10, results.size());
    }

    /**
     * Test of processData method, of class KuduHelper.
     * @throws java.lang.Exception
     */
    @Test()
    public void testProcessData_String() throws Exception {
        System.out.println("processData");
        
        List<RowClass> rows = new ArrayList<>();
        for (int i = 1; i <= 30; ++i) {
            RowClass aRow = new RowClass();
            aRow.setId(i);
            aRow.setAge(30 + i);
            aRow.setName("Joe" + (2 * i));
            
            rows.add(aRow);
        }
        
        Gson gson = new Gson();
        String jsonArray = gson.toJson(rows);
        System.out.println(jsonArray);
        KuduHelper instance = KuduHelperFactory
                .buildKuduHelper("person", new String[]{"cdd7.sis.personal.net.py:7051"});
        instance.init(KuduHelper.KUDU_SESSION_ASYNC, SessionConfiguration.FlushMode.AUTO_FLUSH_BACKGROUND);
        instance.processJson(jsonArray);
    }
    
    /**
     * Test of processData method, of class KuduHelper.
     * @throws java.lang.Exception
     */
    @Test()
    public void testProcessData_ListBaseRow() throws Exception {
        List<BaseRow> rows = new ArrayList<>();
        for (int i = 100; i <= 130; ++i) {
            RowClass aRow = new RowClass();
            aRow.setId(i);
            aRow.setAge(100 + i);
            aRow.setName("Joe" + (2 * i));
            
            rows.add(aRow);
        }
        
        KuduHelper instance = KuduHelperFactory
                .buildKuduHelper("person", new String[]{"cdd7.sis.personal.net.py:7051"});
        instance.init(KuduHelper.KUDU_SESSION_ASYNC, SessionConfiguration.FlushMode.AUTO_FLUSH_BACKGROUND);
        instance.processPojos(rows);
    }
    
    @Test
    public void testGetColumnsTypePair_ListMap() throws Exception {
        List<Map<String, Type>> expected = new ArrayList<>();
        Map<String, Type> colId = new HashMap<>();
        colId.put("id", Type.INT32);
        
        expected.add(colId);
        
        Map<String, Type> colName = new HashMap<>();
        colName.put("name", Type.STRING);
        expected.add(colName);
        
        Map<String, Type> colAge = new HashMap<>();
        colAge.put("age", Type.INT32);
        expected.add(colAge);
        
        KuduHelper instance = KuduHelperFactory
                .buildKuduHelper("person", new String[] {"cdd7.sis.personal.net.py:7051"});
        instance.init(KuduHelper.KUDU_SESSION_ASYNC, SessionConfiguration.FlushMode.AUTO_FLUSH_BACKGROUND);
        List<Map<String, Type>> columnsInfo = instance.getColumnsTypePair();
        
        for (Map<String, Type> aColumn : columnsInfo) {        
            for (Map.Entry<String, Type> entry : aColumn.entrySet()) {
                System.out.print(entry.getKey() + " : " + entry.getValue().getName());
            }
        }
        Assert.assertEquals(expected, columnsInfo);
    }
}
