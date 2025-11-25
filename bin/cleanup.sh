#!/usr/bin/env bash

set -euo pipefail

SCRIPT_DIR=$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )

k3d cluster delete "${K3D_CLUSTER_NAME}" 

K8S_DIR="$SCRIPT_DIR/../k8s"

rm -rf "$K8S_DIR/features/polaris.yaml"
rm -rf "$K8S_DIR/features/postgresql.yaml"
rm -rf  "$K8S_DIR/polaris/polaris-secrets.yaml"
rm -rf "$K8S_DIR/polaris/.bootstrap-credentials.env"
rm -rf "$K8S_DIR/polaris/.polaris.env"
rm -rf "$K8S_DIR/polaris/rsa_key"
rm -rf "$K8S_DIR/polaris/rsa_key.pub"