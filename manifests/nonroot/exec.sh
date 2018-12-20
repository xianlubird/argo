#!/usr/bin/env bash+#!/bin/bash
+set -xe
+
+id
+#printenv
+
+export KUBERNETES_SERVICE_HOST=kubernetes.default.svc.cluster.local
+export KUBERNETES_SERVICE_PORT=443
+
+sudo env ARGO_POD_NAME=$ARGO_POD_NAME KUBERNETES_SERVICE_PORT=$KUBERNETES_SERVICE_PORT KUBERNETES_SERVICE_HOST=$KUBERNETES_SERVICE_HOST \
    bash -c 'argoexec wait'