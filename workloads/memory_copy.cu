#include <torch/extension.h>
#include <cuda_runtime.h>

__global__ void copy_kernel(const float* src, float* dst, int n) {
    int idx = blockIdx.x * blockDim.x + threadIdx.x;
    if (idx < n) {
        dst[idx] = src[idx];
    }
}

void launch_copy(torch::Tensor src, torch::Tensor dst) {
    TORCH_CHECK(src.is_cuda() && dst.is_cuda(), "tensors must be CUDA");
    TORCH_CHECK(src.scalar_type() == torch::kFloat32 && dst.scalar_type() == torch::kFloat32,
                "only float32 is supported");
    TORCH_CHECK(src.numel() == dst.numel(), "src/dst size mismatch");

    int n = static_cast<int>(src.numel());
    int block = 256;
    int grid = (n + block - 1) / block;
    copy_kernel<<<grid, block>>>(src.data_ptr<float>(), dst.data_ptr<float>(), n);
}

PYBIND11_MODULE(TORCH_EXTENSION_NAME, m) {
    m.def("launch_copy", &launch_copy, "Memory copy kernel launch");
}
