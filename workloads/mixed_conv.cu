#include <torch/extension.h>
#include <cuda_runtime.h>

__global__ void conv2d_nchw_kernel(
    const float* x,
    const float* w,
    float* y,
    int n,
    int in_c,
    int out_c,
    int h,
    int w_in,
    int k,
    int out_h,
    int out_w,
    int stride,
    int pad
) {
    int idx = blockIdx.x * blockDim.x + threadIdx.x;
    int total = n * out_c * out_h * out_w;
    if (idx >= total) return;

    int ow = idx % out_w;
    int oh = (idx / out_w) % out_h;
    int oc = (idx / (out_w * out_h)) % out_c;
    int bn = idx / (out_w * out_h * out_c);

    float acc = 0.0f;
    for (int ic = 0; ic < in_c; ++ic) {
        for (int kh = 0; kh < k; ++kh) {
            for (int kw = 0; kw < k; ++kw) {
                int ih = oh * stride + kh - pad;
                int iw = ow * stride + kw - pad;
                if (ih >= 0 && ih < h && iw >= 0 && iw < w_in) {
                    int x_idx = ((bn * in_c + ic) * h + ih) * w_in + iw;
                    int w_idx = ((oc * in_c + ic) * k + kh) * k + kw;
                    acc += x[x_idx] * w[w_idx];
                }
            }
        }
    }

    int y_idx = ((bn * out_c + oc) * out_h + oh) * out_w + ow;
    y[y_idx] = acc;
}

void launch_conv(torch::Tensor input, torch::Tensor weight, torch::Tensor output, int stride, int pad) {
    TORCH_CHECK(input.is_cuda() && weight.is_cuda() && output.is_cuda(), "tensors must be CUDA");
    TORCH_CHECK(input.scalar_type() == torch::kFloat32 && weight.scalar_type() == torch::kFloat32 && output.scalar_type() == torch::kFloat32,
                "only float32 is supported");
    TORCH_CHECK(input.dim() == 4 && weight.dim() == 4 && output.dim() == 4, "expected NCHW tensors");

    int n = static_cast<int>(input.size(0));
    int in_c = static_cast<int>(input.size(1));
    int h = static_cast<int>(input.size(2));
    int w_in = static_cast<int>(input.size(3));
    int out_c = static_cast<int>(weight.size(0));
    int k = static_cast<int>(weight.size(2));
    int out_h = static_cast<int>(output.size(2));
    int out_w = static_cast<int>(output.size(3));

    int total = n * out_c * out_h * out_w;
    int block = 256;
    int grid = (total + block - 1) / block;

    conv2d_nchw_kernel<<<grid, block>>>(
        input.data_ptr<float>(),
        weight.data_ptr<float>(),
        output.data_ptr<float>(),
        n,
        in_c,
        out_c,
        h,
        w_in,
        k,
        out_h,
        out_w,
        stride,
        pad
    );
}

PYBIND11_MODULE(TORCH_EXTENSION_NAME, m) {
    m.def("launch_conv", &launch_conv, "Conv2D kernel launch");
}
