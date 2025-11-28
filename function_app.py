import azure.functions as func
import logging
import pandas as pd
import io

app = func.FunctionApp()

@app.blob_trigger(
    arg_name="myblob",
    path="raw/{name}",
    connection="AzureWebJobsStorage"
)
@app.blob_output(
    arg_name="outputblob",
    path="processados/{name}",
    connection="AzureWebJobsStorage"
)
def blob_trigger(myblob: func.InputStream, outputblob: func.Out[bytes]):
    logging.info(
        f"Python blob trigger function processed blob "
        f"Name: {myblob.name} "
        f"Blob Size: {myblob.length} bytes"
    )
    
    # Verificar se é um arquivo XLSX
    if not myblob.name.lower().endswith('.xlsx'):
        logging.info(f"Arquivo {myblob.name} não é XLSX. Ignorando.")
        return
    
    # Ignorar arquivos que já foram processados (têm "_analise" no nome)
    blob_name = myblob.name
    file_name = blob_name.split('/')[-1]  # "nome_arquivo.xlsx"
    if '_analise' in file_name.lower():
        logging.info(f"Arquivo {myblob.name} já foi processado. Ignorando.")
        return
    
    try:
        # Ler o arquivo Excel do blob
        excel_data = myblob.read()
        df = pd.read_excel(io.BytesIO(excel_data), header=None)
        
        # Selecionar apenas as duas primeiras colunas (caso o arquivo tenha mais colunas)
        if len(df.columns) > 2:
            df = df.iloc[:, :2]  # Seleciona apenas as duas primeiras colunas
        
        # Definir nomes das colunas
        df.columns = ["Categoria", "Valor"]
        
        # Verificar se existe a linha "Salario" no arquivo
        salario_rows = df.loc[df["Categoria"] == "Salario", "Valor"]
        if len(salario_rows) == 0:
            logging.error(f"Arquivo {myblob.name} não contém a linha 'Salario'. Ignorando.")
            return



        # Obtendo o salário
        salario = salario_rows.values[0]
        
        # Separando somente os gastos
        df_gastos = df[df["Categoria"] != "Salario"].copy()
        
        # Calculando percentuais
        df_gastos["Percentual"] = (df_gastos["Valor"] / salario) * 100
        
        # Função de recomendação
        def gerar_recomendacao(categoria, percentual):
            if categoria == "Aluguel":
                if percentual > 30:
                    return "Aluguel muito alto. Considere renegociar."
                return "Aluguel ideal."
            
            if categoria == "Mercado":
                if percentual > 12:
                    return "Mercado alto. Faça compras planejadas."
                return "Mercado ideal."
            
            if categoria == "Conta de Luz":
                if percentual > 5:
                    return "Luz alta. Verifique consumo."
                return "Luz ideal."
            
            if categoria == "Conta de Água":
                if percentual > 4:
                    return "Água alta. Verifique consumo."
                return "Água ideal."
            
            if categoria == "Outros":
                if percentual > 10:
                    return "Gasto em 'Outros' acima do ideal. Reveja despesas extras."
                return "Gasto em 'Outros' dentro do limite."
            
            return ""   # caso apareça outra categoria não prevista
        
        # Aplicando recomendações
        df_gastos["Recomendacao"] = df_gastos.apply(
            lambda linha: gerar_recomendacao(linha["Categoria"], linha["Percentual"]),
            axis=1
        )
        
        # Preparar o arquivo Excel em memória
        output = io.BytesIO()
        df_gastos.to_excel(output, index=False, engine='openpyxl')
        output.seek(0)
        
        # Processar o nome do arquivo: remover extensão, adicionar "_analise" e extensão novamente
        file_name = blob_name.split('/')[-1]  # "nome_arquivo.xlsx"
        base_name = file_name.replace('.xlsx', '').replace('.XLSX', '')
        output_file_name = f"{base_name}_analise.xlsx"
        output_path = f"processados/{output_file_name}"  # Salvar no container "processados"
        
        # Preparar dados para upload
        output_data = output.read()
        logging.info(f"Preparando para salvar arquivo: {output_file_name} (tamanho: {len(output_data)} bytes)")
        
        # Usar requests para fazer upload direto (evita problema com cryptography)
        import os
        import requests
        from urllib.parse import quote
        from datetime import datetime
        import hmac
        import hashlib
        import base64
        
        connection_string = os.environ.get("AzureWebJobsStorage")
        conn_parts = dict(part.split('=', 1) for part in connection_string.split(';') if '=' in part)
        account_name = conn_parts.get('AccountName', '')
        account_key = conn_parts.get('AccountKey', '')
        endpoint_suffix = conn_parts.get('EndpointSuffix', 'core.windows.net')
        
        container_name = "processados"
        blob_url = f"https://{account_name}.blob.{endpoint_suffix}/{container_name}/{quote(output_file_name, safe='')}"
        logging.info(f"Tentando fazer upload para: {blob_url}")
        
        # Criar assinatura para PUT (upload)
        date_str = datetime.utcnow().strftime('%a, %d %b %Y %H:%M:%S GMT')
        blob_path = f"/{account_name}/{container_name}/{output_file_name}"
        
        # String de assinatura para Blob Service PUT (formato correto)
        # A ordem dos campos na string de assinatura é importante!
        canonicalized_headers = f"x-ms-blob-type:BlockBlob\nx-ms-date:{date_str}\nx-ms-version:2021-04-10"
        canonicalized_resource = blob_path
        
        # Formato correto da string de assinatura para PUT blob
        string_to_sign = (
            f"PUT\n"  # HTTP Verb
            f"\n"  # Content-Encoding
            f"\n"  # Content-Language
            f"{len(output_data)}\n"  # Content-Length
            f"\n"  # Content-MD5
            f"application/vnd.openxmlformats-officedocument.spreadsheetml.sheet\n"  # Content-Type
            f"\n"  # Date
            f"\n"  # If-Modified-Since
            f"\n"  # If-Match
            f"\n"  # If-None-Match
            f"\n"  # If-Unmodified-Since
            f"\n"  # Range
            f"{canonicalized_headers}\n"  # CanonicalizedHeaders
            f"{canonicalized_resource}"  # CanonicalizedResource
        )
        
        signature = base64.b64encode(
            hmac.new(
                base64.b64decode(account_key),
                string_to_sign.encode('utf-8'),
                hashlib.sha256
            ).digest()
        ).decode('utf-8')
        
        headers = {
            'x-ms-date': date_str,
            'x-ms-version': '2021-04-10',
            'x-ms-blob-type': 'BlockBlob',
            'Authorization': f'SharedKey {account_name}:{signature}',
            'Content-Length': str(len(output_data)),
            'Content-Type': 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet'
        }
        
        # Tentar fazer upload direto com nome correto primeiro
        logging.info(f"Tentando fazer upload com nome correto: {output_file_name}")
        upload_success = False
        try:
            response = requests.put(blob_url, data=output_data, headers=headers, timeout=30)
            logging.info(f"Resposta do upload: {response.status_code}")
            if response.status_code in [200, 201]:
                logging.info(f"Arquivo processado salvo com sucesso como: {output_file_name}")
                upload_success = True
            else:
                logging.warning(f"Upload direto falhou ({response.status_code}): {response.text[:500]}")
        except Exception as upload_error:
            logging.warning(f"Erro ao fazer upload direto: {str(upload_error)}")
        
        # Se o upload direto falhou, usar binding de output como fallback
        if not upload_success:
            logging.info("Usando binding de output como fallback...")
            try:
                outputblob.set(output_data)
                logging.info(f"Arquivo salvo usando binding de output (nome: {file_name})")
                logging.warning(f"NOTA: O arquivo foi salvo com nome '{file_name}' em vez de '{output_file_name}' devido a problemas com a assinatura REST API")
            except Exception as binding_error:
                logging.error(f"Erro também ao salvar com binding de output: {str(binding_error)}")
                raise Exception(f"Não foi possível salvar o arquivo. Upload direto falhou e binding também falhou: {str(binding_error)}")
        
    except Exception as e:
        logging.error(f"Erro ao processar arquivo {myblob.name}: {str(e)}")
        raise


# This example uses SDK types to directly access the underlying BlobClient object provided by the Blob storage trigger.
# To use, uncomment the section below and add azurefunctions-extensions-bindings-blob to your requirements.txt file
# Ref: aka.ms/functions-sdk-blob-python
#
# import azurefunctions.extensions.bindings.blob as blob
# @app.blob_trigger(arg_name="client", path="contastoragedenis",
#                   connection="AzureWebJobsStorage")
# def BlobTrigger(client: blob.BlobClient):
#     logging.info(
#         f"Python blob trigger function processed blob \n"
#         f"Properties: {client.get_blob_properties()}\n"
#         f"Blob content head: {client.download_blob().read(size=1)}"
#     )
