# 使用官方的 Python 镜像作为基础镜像
FROM python:3.9

# 设置工作目录
WORKDIR /app

# 将当前目录中的文件复制到容器中的 /app 目录
COPY . /app

# 如果有依赖文件 requirements.txt，复制并安装依赖
COPY requirements.txt /app
RUN pip install --no-cache-dir -r requirements.txt

# 如果没有依赖文件，也可以直接手动安装依赖
# RUN pip install <your_dependencies>

# 将 video-115-302.py 作为入口文件执行
CMD ["python", "video-115-302.py"]

