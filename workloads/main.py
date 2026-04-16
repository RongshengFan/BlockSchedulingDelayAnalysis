import argparse
import os
import sys

import torch

current_dir = os.path.dirname(os.path.abspath(__file__))
if current_dir not in sys.path:
    sys.path.insert(0, current_dir)

import compute_gemm
import memory_copy
import mixed_conv
import sparse_spmv
import vgg16_pool


def run_compute(batch: int, iters: int):
    m = batch * 16
    n = 1024
    k = 4096
    a = torch.randn(m, k, device="cuda", dtype=torch.float32)
    b = torch.randn(k, n, device="cuda", dtype=torch.float32)
    c = torch.empty(m, n, device="cuda", dtype=torch.float32)
    for _ in range(iters):
        compute_gemm.launch_gemm(a, b, c)
    torch.cuda.synchronize()
    print(f"[compute] batch={batch}, shape=({m},{k})x({k},{n}), blocks~={batch * 64}")


def run_memory(batch: int, iters: int):
    numel = batch * 16384
    src = torch.randn(numel, device="cuda", dtype=torch.float32)
    dst = torch.empty_like(src)
    for _ in range(iters):
        memory_copy.launch_copy(src, dst)
    torch.cuda.synchronize()
    print(f"[memory] batch={batch}, numel={numel}")


def run_mixed(batch: int, iters: int):
    in_c, out_c, h, w, kernel = 32, 16, 34, 34, 3
    out_h = h - kernel + 1
    out_w = w - kernel + 1
    input_tensor = torch.randn(batch, in_c, h, w, device="cuda", dtype=torch.float32)
    weight = torch.randn(out_c, in_c, kernel, kernel, device="cuda", dtype=torch.float32)
    output = torch.empty(batch, out_c, out_h, out_w, device="cuda", dtype=torch.float32)
    for _ in range(iters):
        mixed_conv.launch_conv(input_tensor, weight, output, 1, 1)
    torch.cuda.synchronize()
    print(f"[mixed] batch={batch}, input=({batch},{in_c},{h},{w})")


def run_sparse(batch: int, iters: int):
    rows = batch * 64
    cols = 4096
    row_nnz: list[int] = []
    col_idx_host: list[int] = []
    row_offsets_host = [0]

    for row in range(rows):
        bucket = row % 16
        if bucket == 0:
            nnz = 224
        elif bucket < 4:
            nnz = 128
        elif bucket < 8:
            nnz = 64
        elif bucket < 12:
            nnz = 32
        else:
            nnz = 12

        row_nnz.append(nnz)
        row_offsets_host.append(row_offsets_host[-1] + nnz)

        cluster_base = (row * 131 + 17) % cols
        scatter_base = (row * 977 + 29) % cols
        for idx in range(nnz):
            if idx < nnz * 3 // 4:
                col = (cluster_base + (idx * 3 + row) % 96) % cols
            else:
                col = (scatter_base + idx * 211 + row * 7) % cols
            col_idx_host.append(col)

    row_offsets = torch.tensor(row_offsets_host, device="cuda", dtype=torch.int32)
    col_idx = torch.tensor(col_idx_host, device="cuda", dtype=torch.int32)
    values = torch.randn(len(col_idx_host), device="cuda", dtype=torch.float32)
    dense = torch.randn(cols, device="cuda", dtype=torch.float32)
    out = torch.empty(rows, device="cuda", dtype=torch.float32)
    nnz_min = min(row_nnz)
    nnz_max = max(row_nnz)
    nnz_avg = sum(row_nnz) / len(row_nnz)

    for _ in range(iters):
        sparse_spmv.launch_sparse_spmv(row_offsets, col_idx, values, dense, out)
    torch.cuda.synchronize()
    print(
        f"[sparse] batch={batch}, rows={rows}, cols={cols}, blocks~={rows}, "
        f"nnz[min/avg/max]=[{nnz_min}/{nnz_avg:.1f}/{nnz_max}]"
    )


def run_vgg16(batch: int, iters: int):
    # Keep conv launches around 64*batch blocks for comparability with other workloads.
    # For conv kernel: blocks ~= batch * out_c * h * w / 256 when using same-shape conv.
    groups = [
        [16, 16],
        [64, 64],
        [256, 256, 256],
    ]

    def conv_blocks(bs: int, out_c: int, h: int, w: int) -> int:
        return (bs * out_c * h * w + 255) // 256

    def pool_blocks(bs: int, c: int, h: int, w: int) -> int:
        return (bs * c * (h // 2) * (w // 2) + 255) // 256

    est_conv = [64 * batch]
    est_pool = [16 * batch]
    print(
        f"[vgg16] target conv blocks ~= {64 * batch} per launch; "
        f"pool blocks ~= {16 * batch} per launch"
    )

    for _ in range(iters):
        # Start from 32x32 so grouped convs can maintain block scale near 64*batch.
        x = torch.randn(batch, 16, 32, 32, device="cuda", dtype=torch.float32)
        for gi, group in enumerate(groups):
            for out_c in group:
                in_c = int(x.size(1))
                h = int(x.size(2))
                w = int(x.size(3))
                weight = torch.randn(out_c, in_c, 3, 3, device="cuda", dtype=torch.float32)
                # Use pad=1 and same-shape output to keep per-launch grid scale stable.
                out = torch.empty(batch, out_c, h, w, device="cuda", dtype=torch.float32)
                mixed_conv.launch_conv(x, weight, out, 1, 1)
                est_conv.append(conv_blocks(batch, out_c, h, w))
                x = out

            if gi < len(groups) - 1:
                ph = int(x.size(2)) // 2
                pw = int(x.size(3)) // 2
                pooled = torch.empty(
                    batch,
                    int(x.size(1)),
                    ph,
                    pw,
                    device="cuda",
                    dtype=torch.float32,
                )
                vgg16_pool.launch_vgg16_pool(x, pooled)
                est_pool.append(pool_blocks(batch, int(x.size(1)), int(x.size(2)), int(x.size(3))))
                x = pooled

    torch.cuda.synchronize()
    conv_min = min(est_conv)
    conv_max = max(est_conv)
    pool_min = min(est_pool)
    pool_max = max(est_pool)
    print(
        f"[vgg16] batch={batch}, final_shape={tuple(x.shape)}, "
        f"conv_blocks_est=[{conv_min},{conv_max}], pool_blocks_est=[{pool_min},{pool_max}]"
    )


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--workload",
        type=str,
        required=True,
        choices=["compute", "memory", "mixed", "sparse", "vgg16"],
    )
    parser.add_argument("--batch", type=int, required=True)
    parser.add_argument("--iters", type=int, default=8)
    args = parser.parse_args()

    torch.manual_seed(42)
    if args.workload == "compute":
        run_compute(args.batch, args.iters)
    elif args.workload == "memory":
        run_memory(args.batch, args.iters)
    elif args.workload == "mixed":
        run_mixed(args.batch, args.iters)
    elif args.workload == "sparse":
        run_sparse(args.batch, args.iters)
    elif args.workload == "vgg16":
        run_vgg16(args.batch, args.iters)
    else:
        run_sparse(args.batch, args.iters)
