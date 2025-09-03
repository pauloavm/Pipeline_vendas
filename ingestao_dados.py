import csv
import os
from dotenv import load_dotenv
import mysql.connector

# --- INÍCIO: Carregamento das Credenciais ---
load_dotenv()
DB_CONFIG = {
    "host": os.getenv("DB_HOST"),
    "port": os.getenv("DB_PORT"),
    "user": os.getenv("DB_USER"),
    "password": os.getenv("DB_PASSWORD"),
    "database": os.getenv("DB_NAME"),
}
# --- FIM: Carregamento das Credenciais ---


def inserir_dados_do_csv(cursor, caminho_arquivo, sql_insert):
    """Função genérica para ler um CSV e inserir os dados no banco."""
    print(f"Iniciando a ingestão do arquivo: {caminho_arquivo}...")
    try:
        with open(caminho_arquivo, "r", encoding="utf-8") as csvfile:
            reader = csv.reader(csvfile)
            next(reader)  # Pular o cabeçalho

            lote = []
            contador = 0
            for linha in reader:
                lote.append(tuple(linha))
                contador += 1

                # Inserir em lotes de 1000
                if len(lote) == 1000:
                    cursor.executemany(sql_insert, lote)
                    print(f"--> {cursor.rowcount} registos inseridos.")
                    lote = []

            # Inserir o lote final que pode ser menor que 1000
            if lote:
                cursor.executemany(sql_insert, lote)
                print(f"--> {cursor.rowcount} registos inseridos.")

        print(
            f"Ingestão do arquivo {caminho_arquivo} concluída. Total de {contador} linhas processadas."
        )
        return True
    except FileNotFoundError:
        print(f"Erro: O arquivo {caminho_arquivo} não foi encontrado.")
        return False
    except Exception as e:
        print(f"Ocorreu um erro durante a ingestão de {caminho_arquivo}: {e}")
        return False


# --- Bloco Principal de Execução ---
if __name__ == "__main__":
    if not all(DB_CONFIG.values()):
        print("Erro: Credenciais do banco de dados incompletas no arquivo .env.")
        exit()

    conn = None
    try:
        print("Conectando ao banco de dados MySQL...")
        conn = mysql.connector.connect(**DB_CONFIG)
        cursor = conn.cursor()
        print("Conexão bem-sucedida.")

        # Ordem de ingestão é crucial por causa das chaves estrangeiras

        # 1. Inserir Produtos
        sql_produtos = "INSERT INTO Produtos (id_produto, nome, categoria, preco_unitario) VALUES (%s, %s, %s, %s)"
        if inserir_dados_do_csv(cursor, "data/produtos.csv", sql_produtos):
            conn.commit()

        # 2. Inserir Clientes
        sql_clientes = "INSERT INTO Clientes (id_cliente, nome, email, pais) VALUES (%s, %s, %s, %s)"
        if inserir_dados_do_csv(cursor, "data/clientes.csv", sql_clientes):
            conn.commit()

        # 3. Inserir Vendas
        sql_vendas = "INSERT INTO Vendas (id_venda, data_venda, id_cliente, id_produto, quantidade, total_venda) VALUES (%s, %s, %s, %s, %s, %s)"
        if inserir_dados_do_csv(cursor, "data/vendas.csv", sql_vendas):
            conn.commit()

        print("\nProcesso de ingestão de dados concluído com sucesso!")

    except mysql.connector.Error as err:
        print(f"Erro de banco de dados: {err}")
        if conn:
            conn.rollback()  # Desfaz a transação em caso de erro
    finally:
        if conn and conn.is_connected():
            cursor.close()
            conn.close()
            print("Conexão com o MySQL foi fechada.")
