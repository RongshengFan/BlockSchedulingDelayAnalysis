from setuptools import setup
from torch.utils.cpp_extension import BuildExtension, CUDAExtension

common_nvcc_flags = [
    "-O2",
    "-lineinfo",
    "-gencode=arch=compute_89,code=sm_89",
    "-gencode=arch=compute_89,code=compute_89",
]

ext_modules = [
    CUDAExtension(
        name="compute_gemm",
        sources=["compute_gemm.cu"],
        extra_compile_args={"cxx": ["-O2"], "nvcc": common_nvcc_flags},
    ),
    CUDAExtension(
        name="memory_copy",
        sources=["memory_copy.cu"],
        extra_compile_args={"cxx": ["-O2"], "nvcc": common_nvcc_flags},
    ),
    CUDAExtension(
        name="mixed_conv",
        sources=["mixed_conv.cu"],
        extra_compile_args={"cxx": ["-O2"], "nvcc": common_nvcc_flags},
    ),
    CUDAExtension(
        name="sparse_spmv",
        sources=["sparse_spmv.cu"],
        extra_compile_args={"cxx": ["-O2"], "nvcc": common_nvcc_flags},
    ),
    CUDAExtension(
        name="vgg16_pool",
        sources=["vgg16_pool.cu"],
        extra_compile_args={"cxx": ["-O2"], "nvcc": common_nvcc_flags},
    ),
]

setup(name="workloads", version="0.1", ext_modules=ext_modules, cmdclass={"build_ext": BuildExtension})
