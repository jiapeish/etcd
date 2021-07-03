https://coreos.com/blog/running-etcd-in-containers.html

https://quay.io/repository/coreos/etcd?tag=latest&tab=tags


export PUBLIC_IP=127.0.0.1

docker run -d -p 8001:8001 -p 5001:5001 quay.io/coreos/etcd:v3.3.19 -peer-addr ${PUBLIC_IP}:8001 -addr ${PUBLIC_IP}:5001 -name etcd-node1

docker run -d -p 8002:8002 -p 5002:5002 quay.io/coreos/etcd:v3.3.19 -peer-addr ${PUBLIC_IP}:8002 -addr ${PUBLIC_IP}:5002 -name etcd-node2 -peers ${PUBLIC_IP}:8001,${PUBLIC_IP}:8002,${PUBLIC_IP}:8003

docker run -d -p 8003:8003 -p 5003:5003 quay.io/coreos/etcd:v3.3.19 -peer-addr ${PUBLIC_IP}:8003 -addr ${PUBLIC_IP}:5003 -name etcd-node3 -peers ${PUBLIC_IP}:8001,${PUBLIC_IP}:8002,${PUBLIC_IP}:8003


------------------------------------------------------------------------------------------

docker run -d quay.io/coreos/etcd:v3.3.19

docker exec -ti b36d /usr/local/bin/etcd version

------------------------------------------------------------------------------------------

========= local cluster 1 node

./etcd --name infra0 --initial-advertise-peer-urls http://127.0.0.1:2380 \
  --listen-peer-urls http://127.0.0.1:2380 \
  --listen-client-urls http://127.0.0.1:2379 \
  --advertise-client-urls http://127.0.0.1:2379 \
  --initial-cluster infra0=http://127.0.0.1:2380 \
  --initial-cluster-state new


ETCDCTL_API=3 ./etcdctl --endpoints=127.0.0.1:2379 -w table endpoint status 
ETCDCTL_API=3 ./etcdctl --endpoints=127.0.0.1:2379 get --prefix ""
ETCDCTL_API=3 ./etcdctl --endpoints=127.0.0.1:2379 put foo bar

ETCDCTL_API=3 ./etcdctl --endpoints=127.0.0.1:2379 lease grant 120



========= local cluster 3 nodes

./etcd --name infra0 --initial-advertise-peer-urls http://127.0.0.1:2380 \
  --listen-peer-urls http://127.0.0.1:2380 \
  --listen-client-urls http://127.0.0.1:2379 \
  --advertise-client-urls http://127.0.0.1:2379 \
  --initial-cluster infra0=http://127.0.0.1:2380,infra1=http://127.0.0.1:2480,infra2=http://127.0.0.1:2580 \
  --initial-cluster-state new

./etcd --name infra1 --initial-advertise-peer-urls http://127.0.0.1:2480 \
  --listen-peer-urls http://127.0.0.1:2480 \
  --listen-client-urls http://127.0.0.1:2479 \
  --advertise-client-urls http://127.0.0.1:2479 \
  --initial-cluster infra0=http://127.0.0.1:2380,infra1=http://127.0.0.1:2480,infra2=http://127.0.0.1:2580 \
  --initial-cluster-state new

./etcd --name infra2 --initial-advertise-peer-urls http://127.0.0.1:2580 \
  --listen-peer-urls http://127.0.0.1:2580 \
  --listen-client-urls http://127.0.0.1:2579 \
  --advertise-client-urls http://127.0.0.1:2579 \
  --initial-cluster infra0=http://127.0.0.1:2380,infra1=http://127.0.0.1:2480,infra2=http://127.0.0.1:2580 \
  --initial-cluster-state new




ETCDCTL_API=3 ./etcdctl --endpoints=127.0.0.1:2379,127.0.0.1:2479,127.0.0.1:2579 -w table endpoint status

ETCDCTL_API=3 ./etcdctl --endpoints=127.0.0.1:2379,127.0.0.1:2479,127.0.0.1:2579 get --prefix ""

ETCDCTL_API=3 ./etcdctl --endpoints=127.0.0.1:2379,127.0.0.1:2479,127.0.0.1:2579 put leader node1

ETCDCTL_API=3 ./etcdctl --endpoints=127.0.0.1:2379,127.0.0.1:2479,127.0.0.1:2579 lease grant 120

ETCDCTL_API=3 ./etcdctl --endpoints=127.0.0.1:2379,127.0.0.1:2479,127.0.0.1:2579 put --lease=3ee97a6cb3b6e604 leader node1

ETCDCTL_API=3 ./etcdctl --endpoints=127.0.0.1:2379,127.0.0.1:2479,127.0.0.1:2579 lease timetolive 3ee97a6cb3b6e604

ETCDCTL_API=3 ./etcdctl --endpoints=127.0.0.1:2379,127.0.0.1:2479,127.0.0.1:2579 lease timetolive --keys 3ee97a6cb3b6e604

ETCDCTL_API=3 ./etcdctl --endpoints=127.0.0.1:2379,127.0.0.1:2479,127.0.0.1:2579 lease keep-alive 3ee97a6cb3b6e604








