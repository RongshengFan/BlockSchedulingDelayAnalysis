#include <torch/extension.h>
#include <cuda_runtime.h>

__global__ void gemm_kernel(const float* a, const float* b, float* c, int m, int n, int k) {
    int row = blockIdx.y * blockDim.y + threadIdx.y;
    int col = blockIdx.x * blockDim.x + threadIdx.x;
    if (row >= m || col >= n) return;

    float acc = 0.0f;
    for (int i = 0; i < k; ++i) {
        acc += a[row * k + i] * b[i * n + col];
    }
    c[row * n + col] = acc;
}

void launch_gemm(torch::Tensor a, torch::Tensor b, torch::Tensor c) {
    TORCH_CHECK(a.is_cuda() && b.is_cuda() && c.is_cuda(), "tensors must be CUDA");
    TORCH_CHECK(a.scalar_type() == torch::kFloat32 && b.scalar_type() == torch::kFloat32 && c.scalar_type() == torch::kFloat32,
                "only float32 is supported");
    TORCH_CHECK(a.dim() == 2 && b.dim() == 2 && c.dim() == 2, "expected 2D tensors");
    TORCH_CHECK(a.size(1) == b.size(0), "invalid GEMM shapes");
    TORCH_CHECK(c.size(0) == a.size(0) && c.size(1) == b.size(1), "output shape mismatch");

    int m = static_cast<int>(a.size(0));
    int k = static_cast<int>(a.size(1));
    int n = static_cast<int>(b.size(1));

    dim3 block(16, 16);
    dim3 grid((n + block.x - 1) / block.x, (m + block.y - 1) / block.y);
    gemm_kernel<<<grid, block>>>(a.data_ptr<float>(), b.data_ptr<float>(), c.data_ptr<float>(), m, n, k);
}

PYBIND11_MODULE(TORCH_EXTENSION_NAME, m) {
    m.def("launch_gemm", &launch_gemm, "GEMM kernel launch");
}
