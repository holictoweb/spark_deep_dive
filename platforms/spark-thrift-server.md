

[AWS EMR 가이드](https://aws.amazon.com/ko/premiumsupport/knowledge-center/jdbc-connection-emr/)

- squirrel 설치 [](http://www.squirrelsql.org/#installation)

- 관리자 권한 cmd 
``` shell 
java -jar squirrel-sql-4.2.0-standard.jar
```


prerequisites

``` shell

sudo apt-get install libsasl2-dev
sudo pip install  sasl thrift 
sudo pip install pyhive
```


- python code
``` python
from pyhive import hive

host_name = "172.17.8.142"
port = 10001  #default is 10000
user = "hadoop"
database="default"

def hiveconnection(host_name, port, user, database):
    conn = hive.Connection(host=host_name, port=port, username=user, database=database)
    cur = conn.cursor()
    cur.execute('select name  from demo2 return limit 2')
    result = cur.fetchall()

    return result

# Call above function
output = hiveconnection(host_name, port, user, database)
print(output) 
```
