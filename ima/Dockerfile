# Use uma imagem base do Python
FROM python:3.8

# Defina o diretório de trabalho no contêiner
WORKDIR /app

# Copie os arquivos de código-fonte para o contêiner
COPY . .

# Copie o arquivo .env (ou outro arquivo de configuração) para o contêiner
COPY .env .

# Instale as dependências do aplicativo
RUN pip install -r requirements.txt

# Exponha a porta em que o aplicativo Flask está sendo executado
EXPOSE 5000

# Comando para executar o aplicativo Flask
CMD ["python", "app.py"]
