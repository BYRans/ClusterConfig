RestAPIClient
=============

      This tool call Restful API of Cloudera Manager,and could be used to configure the cluster parameters for each service. Improving the efficiency of the new cluster configuration parameters.



Attention：

1.To edit "config.properties" first, and then execute the "RestClient.java"

2.The "cloudera manager" of "config.properties" is the parameters of the cluster

3.Service configuration is as follows：
serviceName@GroupName
key1=value1
key2=value1

4.If there is a space between the key in "config.properties", then put spaces replaced by "_".For example NameNode Xmx should be written as:
NameNode_Xmx=825955249

6.Only the key recorded in the "realKeyName.properties" could be set.


