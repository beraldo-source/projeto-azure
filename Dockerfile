# Dockerfile para Azure Functions com Azurite
FROM mcr.microsoft.com/azure-functions/python:4-python3.11

# Instalar dependências do sistema
RUN apt-get update && apt-get install -y \
    curl \
    && rm -rf /var/lib/apt/lists/*

# Instalar Node.js e Azurite
RUN curl -fsSL https://deb.nodesource.com/setup_18.x | bash - \
    && apt-get install -y nodejs \
    && npm install -g azurite

# Definir diretório de trabalho
WORKDIR /home/site/wwwroot

# Copiar requirements
COPY requirements.txt .

# Instalar dependências Python
RUN pip install --no-cache-dir -r requirements.txt

# Copiar código da função
COPY . .

# Criar diretório para dados do Azurite
RUN mkdir -p /data/azurite

# Expor portas
# 10000 - Blob Storage
# 10001 - Queue Storage  
# 10002 - Table Storage
# 80 - Azure Functions
EXPOSE 10000 10001 10002 80

# Script de inicialização
COPY start.sh /start.sh
RUN chmod +x /start.sh

CMD ["/start.sh"]