#!/bin/bash
pip install click==8.1.7 &
pip install charset-normalizer==3.3.2 &
pip install certifi==2023.7.22 &
pip install colorama==0.4.6&
pip install databricks-cli==0.17.8 &
pip install databricks-connect==13.3.3 &
pip install databricks-sdk==0.11.0 &
pip install decorator==5.1.1 &
pip install deepdiff==6.6.1 &
pip install googleapis-common-protos==1.61.0 &
pip install grpcio-status==1.59.2 &
pip install grpcio==1.59.2 &
pip install idna==3.4 &
pip install numpy==1.26.1 &
pip install oauthlib==3.2.2 &
pip install ordered-set==4.1.0 &
pip install pandas==2.1.2 &
pip install protobuf==4.25.0 &
pip install py-automapper==1.2.3 &
pip install py4j==0.10.9.7 &
pip install py==1.11.0 &
pip install pyarrow==14.0.0 &
pip install pydantic==1.10.6 &
pip install pyjwt==2.8.0 &
pip install pyspark==3.4.1 &
pip install python-dateutil==2.8.2 &
pip install pytz==2023.3.post1 &
pip install requests==2.31.0 &
pip install retry==0.9.2 &
pip install six==1.16.0 &
pip install tabulate==0.9.0 &
pip install typing-extensions==4.8.0 &
pip install tzdata==2023.3 &
pip install urllib3==1.26.18 &
mvn install com.databricks:spark-xml_2.12:0.16.0 &
mvn install com.microsoft.sqlserver:mssql-jdbc:12.3.0.jre8-preview &
mvn install com.oracle.ojdbc:ojdbc8:19.3.0.0 &
mvn install net.sourceforge.jtds:jtds:1.3.1 &
cp /dbfs/FileStore/jars/39181449_5452_4e2e_839e_7fad45f57f73-mssql_jdbc_12_2_0_jre8-eeae5.jar /databricks/jars/
cp /dbfs/FileStore/jars/08071ad7_5f1e_4791_a85f_2ef6a9a6bb06-nzjdbc_1.jar /databricks/jars/
wait