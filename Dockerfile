# 使用 Python 3.11 作为基础镜像
FROM python:3.11-slim

# 设置工作目录
WORKDIR /app

# 安装系统依赖
RUN apt-get update && apt-get install -y \
    build-essential \
    libssl-dev \
    libffi-dev \
    python3-dev \
    && rm -rf /var/lib/apt/lists/*

# 升级 pip
RUN pip install --upgrade pip

# 复制项目文件到容器中
COPY . /app

# 安装项目依赖
RUN pip install -r requirements.txt

# 启动应用程序
CMD ["python", "updatedb.py"]
CMD ["python", "servedb.py","-fs"]
CMD ["python", "video-115-302.py", "-f", "a.db"]
