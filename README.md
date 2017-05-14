# Specification
* hadoop 2.6.0
* java 8
* Nutch
* jsoup
* using Lucene to query result

# Hadoop command
`hadoop jar [jar_name] [option]`  
allow users to run the job on the hadoop cluster.
#####options:
`-conf specify an application configuration file`  
`-D use value for given property`
`-fs specify a namenode`  
`-jt specify a job tracker`  
`-files specify comma separated files to be copied to the map reduce cluster`  
`-libjars specify comma separated jar files to include in the classpath.`  
`-archives specify comma separated archives to be unarchived on the compute machines.`  