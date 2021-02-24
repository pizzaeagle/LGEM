###kubernetes url

127.0.0.1:8001


### run spark


spark-submit --master k8s://https://kubernetes.docker.internal:6443 \
--deploy-mode cluster \
--name kubernetes-test \
--class mm.graph.embeddings.Main \
--conf spark.executor.instances=1 \
--conf spark.kubernetes.container.image=test:1.0 \
local:///opt/spark/example/jars/lgem.jar

spark-submit --master k8s://https://kubernetes.docker.internal:6443 \
--deploy-mode cluster \
--name kubernetes-test \
--class mm.graph.embeddings.Main \
--conf spark.executor.instances=1 \
--conf spark.kubernetes.container.image=test2:1.0 \
local:///opt/spark/example/jars/lgem.jar


docker run -it <image> /bin/sh
### token
kubectl -n kube-system describe secret default

kubectl apply -f https://raw.githubusercontent.com/kubernetes/dashboard/v2.0.4/aio/deploy/recommended.yaml
kubectl proxy

kubectl delete all --all --all-namespaces

kubectl delete -n default pod --all



kubectl port-forward kubernetes-test-1612980444066-driver 4040:4040