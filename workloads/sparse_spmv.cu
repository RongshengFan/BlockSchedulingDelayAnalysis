#include <torch/extension.h>
#include <cuda_runtime.h>

__global__ void sparse_spmv_kernel(
    const int* row_offsets,
    const int* col_idx,
    const float* values,
    const float* x,
    float* y,
    int rows
) {
    int row = blockIdx.x;
    if (row >= rows) return;

    __shared__ float partial[256];
    int tid = threadIdx.x;
    int start = row_offsets[row];
    int end = row_offsets[row + 1];

    float acc = 0.0f;
    for (int idx = start + tid; idx < end; idx += blockDim.x) {
        int col = col_idx[idx];
        float val = values[idx];
        acc += val * x[col];
    }

    partial[tid] = acc;
    __syncthreads();

    for (int stride = blockDim.x / 2; stride > 0; stride >>= 1) {
        if (tid < stride) {
            partial[tid] += partial[tid + stride];
        }
        __syncthreads();
    }

    if (tid == 0) {
        y[row] = partial[0];
    }
}

void launch_sparse_spmv(
    torch::Tensor row_offsets,
    torch::Tensor col_idx,
    torch::Tensor values,
    torch::Tensor x,
    torch::Tensor y
) {
    TORCH_CHECK(
        row_offsets.is_cuda() && col_idx.is_cuda() && values.is_cuda() && x.is_cuda() && y.is_cuda(),
        "tensors must be CUDA"
    );
    TORCH_CHECK(row_offsets.scalar_type() == torch::kInt32, "row_offsets must be int32");
    TORCH_CHECK(col_idx.scalar_type() == torch::kInt32, "col_idx must be int32");
    TORCH_CHECK(values.scalar_type() == torch::kFloat32 && x.scalar_type() == torch::kFloat32 && y.scalar_type() == torch::kFloat32,
                "values/x/y must be float32");
    TORCH_CHECK(row_offsets.dim() == 1 && col_idx.dim() == 1 && values.dim() == 1 && x.dim() == 1 && y.dim() == 1,
                "expected 1D tensors");
    TORCH_CHECK(row_offsets.numel() >= 2, "row_offsets must contain at least two entries");
    TORCH_CHECK(col_idx.numel() == values.numel(), "col_idx/values size mismatch");

    int rows = static_cast<int>(y.numel());
    TORCH_CHECK(row_offsets.numel() == rows + 1, "row_offsets length must equal rows + 1");

    int block = 256;
    int grid = rows;
    sparse_spmv_kernel<<<grid, block>>>(
        row_offsets.data_ptr<int>(),
        col_idx.data_ptr<int>(),
        values.data_ptr<float>(),
        x.data_ptr<float>(),
        y.data_ptr<float>(),
        rows
    );
}

PYBIND11_MODULE(TORCH_EXTENSION_NAME, m) {
    m.def("launch_sparse_spmv", &launch_sparse_spmv, "Sparse SpMV kernel launch");
}