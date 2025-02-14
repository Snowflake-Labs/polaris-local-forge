#!/usr/bin/env bash

set -euo pipefail

SCRIPT_DIR=$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )

mkdir -p "$(dirname "$KUBECONFIG")"

curl -sSL "https://dl.k8s.io/release/${K3S_VERSION%[+-]*}/bin/$(uname -o | tr '[:upper:]' '[:lower:]')/$(uname -m)/kubectl" -o "$PWD/bin/kubectl"

chmod +x "$PWD/bin/kubectl"

k3d cluster create \
  --config="${SCRIPT_DIR}/../config/cluster-config.yaml"
