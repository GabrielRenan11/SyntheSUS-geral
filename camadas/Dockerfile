FROM python:3.11-slim

# 1. Cria a pasta da Oracle diretamente no container
RUN mkdir -p /root/.oci

# 2. Copia os arquivos de autenticação direto para a pasta correta
# (O asterisco garante que ele vai pegar o arquivo .pem não importa o nome exato)
COPY config /root/.oci/config
COPY *.pem /root/.oci/

WORKDIR /app

# 3. Instala as dependências
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# 4. Copia o resto do projeto
COPY . .

CMD ["python", "ingestao_synthesus.py"]