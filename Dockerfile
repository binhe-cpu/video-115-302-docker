# 使用 Python 3.10 作为基础镜像
FROM python:3.10-slim

# 设置工作目录
WORKDIR /app

# 升级 pip
RUN pip install --upgrade pip

# 复制所有文件到工作目录
COPY . /app

# 安装项目依赖
RUN pip install -r requirements.txt

# 启动应用
CMD ["python", "video-115-302.py", "-f", "a.db"]
