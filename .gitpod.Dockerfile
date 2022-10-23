FROM gitpod/workspace-full

RUN sudo apt-get update && sudo apt-get install -y tmux

# Install fzf
RUN wget "https://github.com/junegunn/fzf/releases/download/0.34.0/fzf-0.34.0-linux_amd64.tar.gz" && \
    tar xzf "fzf-0.34.0-linux_amd64.tar.gz" && \
    sudo mv fzf /usr/bin

# Install Python 3.10
RUN pyenv install 3.10.7 && \
    pyenv global 3.10.7

# Install AWS CLI
RUN curl "https://awscli.amazonaws.com/awscli-exe-linux-x86_64.zip" -o "awscliv2.zip" && \
    unzip awscliv2.zip && \
    sudo ./aws/install
