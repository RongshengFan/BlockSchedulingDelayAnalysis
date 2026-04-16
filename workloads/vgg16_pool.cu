#include <torch/extension.h>
#include <cuda_runtime.h>

__global__ void vgg16_pool_kernel(const float* x, float* y, int n, int c, int h, int w, int out_h, int out_w) {
    int idx = blockIdx.x * blockDim.x + threadIdx.x;
    int total = n * c * out_h * out_w;
    if (idx >= total) return;

    int ow = idx % out_w;
    int oh = (idx / out_w) % out_h;
    int ch = (idx / (out_w * out_h)) % c;
    int bn = idx / (out_w * out_h * c);

    int ih0 = oh * 2;
    int iw0 = ow * 2;

    float v0 = x[((bn * c + ch) * h + ih0) * w + iw0];
    float v1 = x[((bn * c + ch) * h + ih0) * w + iw0 + 1];
    float v2 = x[((bn * c + ch) * h + ih0 + 1) * w + iw0];
    float v3 = x[((bn * c + ch) * h + ih0 + 1) * w + iw0 + 1];

    float m01 = v0 > v1 ? v0 : v1;
    float m23 = v2 > v3 ? v2 : v3;
    y[idx] = m01 > m23 ? m01 : m23;
}

void launch_vgg16_pool(torch::Tensor x, torch::Tensor y) {
    TORCH_CHECK(x.is_cuda() && y.is_cuda(), "tensors must be CUDA");
    TORCH_CHECK(x.scalar_type() == torch::kFloat32 && y.scalar_type() == torch::kFloat32,
                "only float32 is supported");
    TORCH_CHECK(x.dim() == 4 && y.dim() == 4, "expected NCHW tensors");

    int n = static_cast<int>(x.size(0));
    int c = static_cast<int>(x.size(1));
    int h = static_cast<int>(x.size(2));
    int w = static_cast<int>(x.size(3));
    int out_h = static_cast<int>(y.size(2));
    int out_w = static_cast<int>(y.size(3));

    TORCH_CHECK(out_h * 2 == h && out_w * 2 == w, "output must be half in H and W");

    int total = n * c * out_h * out_w;
    int block = 256;
    int grid = (total + block - 1) / block;
    vgg16_pool_kernel<<<grid, block>>>(x.data_ptr<float>(), y.data_ptr<float>(), n, c, h, w, out_h, out_w);
}

PYBIND11_MODULE(TORCH_EXTENSION_NAME, m) {
    m.def("launch_vgg16_pool", &launch_vgg16_pool, "VGG16 2x2 pooling kernel launch");
}
