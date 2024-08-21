
# Consulta de Situação Cadastral de Empresas

## Descrição

Este script Python automatiza a consulta da situação cadastral de empresas utilizando o site BrasilCNPJ.net. Ele lê um arquivo CSV com informações de empresas, gera URLs para consulta e processa as respostas em paralelo, salvando os resultados em um novo arquivo CSV. O programa também gera logs detalhados para monitoramento e organiza os arquivos processados em pastas separadas por data.

### Principais funcionalidades:
- Leitura de CNPJs de um arquivo CSV e geração de URLs para consulta.
- Processamento paralelo das URLs, com extração da "Situação Cadastral".
- Salvamento dos resultados em um novo arquivo CSV.
- Checkpoints periódicos para evitar perda de dados.
- Registro de logs detalhados da execução.

## Como Usar

### 1. Preparar o ambiente
Certifique-se de ter Python 3.x instalado. Instale as dependências necessárias usando `pip`:

\`\`\`bash
pip install requests beautifulsoup4
\`\`\`

### 2. Organize os arquivos
Crie uma pasta chamada `Entrada_arquivos` e coloque o arquivo CSV contendo as empresas a serem consultadas. O arquivo CSV deve ter as colunas `TIPOFJ` e `CGCENT`.

### 3. Executar o script
Rode o script Python:

\`\`\`bash
python nome_do_script.py
\`\`\`

O script irá processar o arquivo CSV e salvar os resultados na pasta `Processados`, organizada por data.

### 4. Verificar logs
Durante a execução, logs detalhados serão gerados na pasta `logs`, permitindo monitorar o progresso e identificar possíveis erros.

## Exemplo de CSV de Entrada

O arquivo CSV deve ter o seguinte formato:

\`\`\`csv
TIPOFJ;CGCENT;OutrasColunas...
J;12.345.678/0001-90;...
F;123.456.789-01;...
\`\`\`

Apenas as linhas com `TIPOFJ` igual a 'J' serão processadas.
