#!/usr/bin/env bash

set -euo pipefail

SCRIPT_DIR=$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )

k3d cluster delete "${K3D_CLUSTER_NAME}" || true

K8S_DIR="$SCRIPT_DIR/../k8s"

rm -f "$K8S_DIR/features/postgresql.yaml"
rm -f "$K8S_DIR/polaris/.bootstrap-credentials.env"
rm -f "$K8S_DIR/polaris/.polaris.env"
rm -f "$K8S_DIR/polaris/persistence.xml"
rm -f "$K8S_DIR/polaris/rsa_key"
rm -f "$K8S_DIR/polaris/rsa_key.pub"