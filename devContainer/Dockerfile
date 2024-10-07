# 基于官方的 Go 镜像
FROM golang:1.20

# 设置工作目录
WORKDIR /workspace

# 安装 SSH 和其他工具
RUN apt-get update && \
    apt-get install -y openssh-server git curl vim

# 生成 SSH 配置，允许 VSCode Remote 连接
RUN mkdir /var/run/sshd && \
    echo 'root:root' | chpasswd && \
    sed -i 's/#PermitRootLogin prohibit-password/PermitRootLogin yes/' /etc/ssh/sshd_config && \
    mkdir -p /root/.ssh && \
    echo "PasswordAuthentication no" >> /etc/ssh/sshd_config && \
    echo "PubkeyAuthentication yes" >> /etc/ssh/sshd_config

# 将本地的 SSH 公钥复制到容器内
COPY id_rsa.pub /root/.ssh/authorized_keys

# 设置合适的权限
RUN chmod 700 /root/.ssh && chmod 600 /root/.ssh/authorized_keys

# 暴露 SSH 端口
EXPOSE 22

# 启动 SSH 服务
CMD ["/usr/sbin/sshd", "-D"]
