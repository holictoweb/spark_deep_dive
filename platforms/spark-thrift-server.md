#### spark thrift server 실행
[AWS EMR 가이드](https://aws.amazon.com/ko/premiumsupport/knowledge-center/jdbc-connection-emr/)

- master node 상에서 thrift server 수행 

``` sh
sudo /usr/lib/spark/sbin/start-thriftserver.sh

```

- squirrel 에서 사용할 class를 다운로드

#### pyhive

[python code 부터 모든 내용 포함](https://sites.google.com/a/ku.th/big-data/pyhive)

- pyhive를 사용 하여 jdbc 연결 후 쿼리 


- aws linux 2
```
sudo yum install java-1.8.0-openjdk
sudo yum install cyrus-sasl-devel
sudo pip install sasl thrift 
sudo pip3 install thrift_sasl
sudo pip install pyhive
```

- ubuntu
``` shell

sudo apt-get install libsasl2-dev
sudo pip install  sasl thrift 
sudo pip3 install thrift_sasl
sudo pip install pyhive
```

#### squirrel 
- jdbc 연결 가능한 data source 에 대한 쿼리 분석기
- squirrel 설치 [windowsOS](http://www.squirrelsql.org/#installation)

- 관리자 권한 cmd 
``` shell 
java -jar squirrel-sql-4.2.0-standard.jar
```




