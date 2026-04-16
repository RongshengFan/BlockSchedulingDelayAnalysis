#!/bin/bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "$0")" && pwd)"
PYTHON=${PYTHON:-/home/lichaoqun/anaconda3/envs/venv/bin/python}
TRACE_ROOT=${TRACE_ROOT:-"$ROOT_DIR/../traces"}
PROBE=${PROBE:-"$ROOT_DIR/../probe/probe.py"}
# Default iterations for simple workloads. VGG16 uses fewer iterations to keep
# total CSV rows in the same magnitude as other workloads.
ITERS=${ITERS:-8}
ITERS_VGG16=${ITERS_VGG16:-1}
BATCHES=${BATCHES:-"8 16 32 64"}
# Use five representative workloads by default. Sparse uses irregular row lengths.
WORKLOADS=${WORKLOADS:-"compute memory mixed sparse vgg16"}

export TORCH_NVCC_FLAGS="-gencode=arch=compute_89,code=sm_89 -gencode=arch=compute_89,code=compute_89 -lineinfo"

cd "$ROOT_DIR"
$PYTHON setup.py build_ext --inplace --force

for workload in $WORKLOADS; do
  for bs in $BATCHES; do
    run_iters="$ITERS"
    if [[ "$workload" == "vgg16" ]]; then
      run_iters="$ITERS_VGG16"
    fi

    out="$TRACE_ROOT/${workload}/bs${bs}"
    mkdir -p "$out"
    cd "$out"
    rm -rf ./trace
    mkdir -p ./trace

    echo "[collect] workload=$workload batch=$bs iters=$run_iters"
    rm -rf ~/.cache/neutrino
    neutrino -p "$PROBE" \
      --tracedir ./trace \
      "$PYTHON" "$ROOT_DIR/main.py" --workload "$workload" --batch "$bs" --iters "$run_iters"

    kept=0
    for td in ./trace/*; do
      if [[ ! -d "$td" ]]; then
        continue
      fi
      if compgen -G "$td/result/*.bin" > /dev/null; then
        kept=$((kept + 1))
      else
        rm -rf "$td"
      fi
    done

    echo "[collect] workload=$workload batch=$bs valid_traces=$kept"
    cd - >/dev/null
  done
done

echo "[done] traces at $TRACE_ROOT"
