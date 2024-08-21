import csv
import os
import sys
import requests
import logging
from datetime import datetime
from bs4 import BeautifulSoup
from concurrent.futures import ThreadPoolExecutor, as_completed

ENTRADA_DIR = "Entrada_arquivos"
PROCESSADOS_DIR = "Processados"
LOGS_DIR = "logs"

if not os.path.exists(LOGS_DIR):
    os.makedirs(LOGS_DIR)
log_filename = os.path.join(LOGS_DIR, f"execucao_{datetime.now().strftime('%Y-%m-%d')}.log")
logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s - %(levelname)s - %(message)s',
                    handlers=[
                        logging.FileHandler(log_filename),
                        logging.StreamHandler(sys.stdout)
                    ])

def ler_urls_do_csv(arquivo_csv):
    dados = []
    caminho_arquivo = os.path.join(ENTRADA_DIR, arquivo_csv)
    
    if not os.path.exists(caminho_arquivo):
        logging.error(f"Erro: O arquivo '{caminho_arquivo}' não existe.")
    else:
        try:
            with open(caminho_arquivo, newline='', encoding='latin1') as arquivo:
                leitor = csv.DictReader(arquivo, delimiter=';')
                if not {'TIPOFJ', 'CGCENT'}.issubset(leitor.fieldnames):
                    logging.error("Erro: O arquivo CSV está faltando uma das colunas obrigatórias.")
                else:
                    for linha in leitor:
                        if linha['TIPOFJ'].lower() == 'j':
                            cnpj = linha['CGCENT'].replace('.', '').replace('/', '').replace('-', '')
                            url = f'https://brasilcnpj.net/cnpj/{cnpj}'
                            linha['URL'] = url
                            linha['STATUS'] = ''
                            dados.append(linha)
                    logging.info(f"Total de URLs lidas: {len(dados)}")
        except Exception as e:
            logging.error(f"Erro ao ler o arquivo CSV: {e}")
    return dados

def processar_url(linha):
    url = linha['URL']
    try:
        response = requests.get(url)
        if response.status_code == 200:
            soup = BeautifulSoup(response.content, 'html.parser')
            
            situacao_cadastral_element = soup.find(lambda tag: tag.name == 'div' and tag.text.startswith("Situação Cadastral:"))
            if situacao_cadastral_element:
                situacao_cadastral = situacao_cadastral_element.text.replace("Situação Cadastral:", "").strip()
                linha['STATUS'] = situacao_cadastral
                logging.info(f"URL: {url} - Situação Cadastral: {situacao_cadastral}")
            else:
                logging.warning(f"Situação Cadastral não encontrada para {url}")
        else:
            print(f"Failed to fetch {url}: Status code {response.status_code}")
    except requests.exceptions.RequestException as e:
        logging.error(f"Erro ao acessar {url}: {e}")

    return linha

def processar_urls(dados, checkpoint=100):
    total = len(dados)
    salvos = 0
    
    with ThreadPoolExecutor(max_workers=10) as executor:
        futures = [executor.submit(processar_url, linha) for linha in dados]
        for future in as_completed(futures):
            try:
                result = future.result()
                linha_index = dados.index(result)
                dados[linha_index] = result
                
                salvos += 1
                if salvos % checkpoint == 0:
                    salvar_csv('PCCLIENT_Processado.csv', dados[:salvos])
                    logging.info(f"Checkpoint: Salvos {salvos} de {total}...")
                    
            except Exception as e:
                logging.error(f"Erro ao processar uma URL: {e}")
                
        salvar_csv('PCCLIENT_Processado.csv', dados)

def salvar_csv(nome_arquivo, dados):
    data_atual = datetime.now().strftime('%Y-%m-%d')
    diretorio_data = os.path.join(PROCESSADOS_DIR, data_atual)

    if not os.path.exists(diretorio_data):
        os.makedirs(diretorio_data)
    
    caminho_arquivo = os.path.join(diretorio_data, nome_arquivo)
    
    try:
        fieldnames = list(dados[0].keys()) if dados else []
        with open(caminho_arquivo, 'w', newline='', encoding='latin1') as arquivo:
            escritor = csv.DictWriter(arquivo, fieldnames=fieldnames, delimiter=';')
            escritor.writeheader()
            for linha in dados:
                escritor.writerow(linha)
        logging.info(f"Arquivo salvo com sucesso em: {caminho_arquivo}")
    except Exception as e:
        logging.error(f"Erro ao salvar o arquivo CSV: {e}")

if __name__ == "__main__":
    arquivo_csv = 'PCCLIENT.csv'
    dados = ler_urls_do_csv(arquivo_csv)
    if dados:
        processar_urls(dados)