����: docker, redis6.2.5


�������ӣ� 

redis-server /opt/softs/redis/conf/redis-6379.conf

redis-server /opt/softs/redis/conf/redis-6380.conf


��Ҫ�������ԣ�dir, pidfile, port, replicaof




����sentinel�ڱ���Ⱥ��

1. ����������

redis-server /opt/softs/redis/conf/redis-6379.conf

redis-server /opt/softs/redis/conf/redis-6380.conf


2. �������ڱ���window����û��redis-sentinel ����

redis-server /opt/softs/redis/conf/sentinel0.conf --sentinel

redis-server /opt/softs/redis/conf/sentinel1.conf --sentinel






cluster��Ⱥ��




redis5��redis6������
1. ...