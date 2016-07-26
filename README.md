# KuduHelper
Java library that may be included in other projects to persist data into a Kudu table. It could also be invoked from command line.
<br/>
<b>When running from command line:</b><br/>

java -cp KuduJDBHelper.jar org.jc.kududbhelper.lib.KuduHelperMain &lt;tableName&gt; &lt;sync | async&gt; &lt;jsonArray&gt; master1:port,master2:port,...,masterN:port

<b>Arguments</b>:<br/>
<ul>
  <li><b>tableName.</b> Name of the table where data is to be inserted.</li>
  <li><b>sync|async.</b> Refers to the type of session kudu will use. Type sync for synchrounous session. Type async for an asynchronous session.</li>
  <li><b>jsonArray.</b> Data to be inserted into the table. It must be encoded as an array of json objects and keys MUST match columns name in table.</li>
  <li><b>master1:port...masterN:port.</b> A list of master servers followed by the port number, separated by commas. Do not use spaces between master addresses.</li>
</ul>
<b>Output:</b><br/>
If everything went fine, the program just displays a message to check your kudu table to make sure data was inserted. However, if something failed, an exception stacktrace will be displayed.
<br/>
<b>When including jar inside another project</b><br/>
Always get an instance of KuduHelper through factory and don't forget to initialize by invoking KuduHelper#init method.
<br/><br/>
<b>Final Notes</b><br/>
<ul>
  <li>I'm uploading a netbeans project and also the jar file (with all libs included) under /store.</li>
  <li>When you use a class to represent each row in a kudu table, make sure that this class extends BaseRow base class, otherwise it might not work properly</li>
</ul>
