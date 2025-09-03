import random
import os
from datetime import datetime
from faker import Faker
import mysql.connector
from dotenv import load_dotenv

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

locales = ["en_US", "pt_BR", "es_ES", "fr_FR", "de_DE", "it_IT", "ru_RU", "pt_PT"]
faker = Faker(locales)

produtos_eletronicos = {
    "Celulares": {
        "iPhone 13": 850.00,
        "Samsung Galaxy S22": 799.00,
        "Google Pixel 6": 699.00,
        "Xiaomi 12": 599.00,
        "OnePlus 10 Pro": 750.00,
    },
    "Acessórios": {
        "Carregador USB-C": 25.00,
        "Capa de Silicone": 15.00,
        "Fone de Ouvido Bluetooth": 50.00,
        "Smartwatch": 150.00,
        "Power Bank 10000mAh": 30.00,
        "Protetor de tela de vidro": 10.00,
    },
}


def carregar_clientes_existentes(cursor):
    """Lê o banco de dados e carrega os clientes já existentes para a memória, com e-mails em minúsculas."""
    print("Carregando clientes existentes do banco de dados...")
    pool = {}
    cursor.execute("SELECT id_cliente, nome, email, pais FROM Clientes")
    for id_cliente, nome, email, pais in cursor.fetchall():
        # <<< CORREÇÃO DE CASE SENSITIVITY >>>
        # Garante que a chave do dicionário seja sempre minúscula
        pool[email.lower()] = {
            "ID_Cliente": id_cliente,
            "Nome_Cliente": nome,
            "Email_Cliente": email,  # Mantemos o email original no valor, se desejado
            "País": pais,
        }
    print(f"{len(pool)} clientes existentes foram carregados.")
    return pool


def popular_produtos(cursor):
    """Insere os produtos do dicionário na tabela Produtos e retorna um dict com os dados do BD."""
    print("Populando a tabela de Produtos...")
    lista_produtos_para_inserir = []
    for categoria, produtos in produtos_eletronicos.items():
        for nome, preco in produtos.items():
            lista_produtos_para_inserir.append((nome, categoria, preco))

    sql = "INSERT IGNORE INTO Produtos (nome, categoria, preco_unitario) VALUES (%s, %s, %s)"
    cursor.executemany(sql, lista_produtos_para_inserir)

    cursor.execute("SELECT id_produto, nome, preco_unitario FROM Produtos")
    produtos_do_banco = cursor.fetchall()
    produtos_dict = {
        nome: {"id": id_prod, "preco": preco}
        for id_prod, nome, preco in produtos_do_banco
    }
    print("Tabela de Produtos populada e carregada.")
    return produtos_dict


def generate_customer_email(customer_name):
    """Gera um e-mail a partir de um nome de cliente."""
    name_parts = customer_name.lower().split()
    first_name = name_parts[0]
    last_name = name_parts[-1] if len(name_parts) > 1 else ""
    email_domains = ["gmail.com", "outlook.com", "yahoo.com", "hotmail.com"]
    base_email = "".join(
        e for e in f"{first_name}.{last_name}" if e.isalnum() or e == "."
    )
    email_domain = random.choice(email_domains)
    return f"{base_email}@{email_domain}"


def generate_customer():
    """Gera um novo cliente fictício, com e-mail em minúsculas."""
    locale = random.choice(locales)
    faker_locale = Faker(locale)
    customer_name = faker_locale.name()
    # <<< CORREÇÃO DE CASE SENSITIVITY >>>
    # Garante que todo e-mail gerado seja sempre minúsculo
    customer_email = generate_customer_email(customer_name).lower()

    return {
        "ID_Cliente": str(faker.uuid4()),
        "Nome_Cliente": customer_name,
        "Email_Cliente": customer_email,
        "País": faker_locale.country(),
    }


def generate_sale_data(sale_id, start_date, end_date, customers_pool, produtos_dict):
    """Gera dados de venda, reutilizando ou criando clientes de forma consistente."""
    cliente_a_ser_inserido_no_bd = None
    customer_data = None

    if random.random() < 0.7 and customers_pool:
        random_email = random.choice(list(customers_pool.keys()))
        customer_data = customers_pool[random_email]
    else:
        novo_cliente_potencial = generate_customer()
        email_novo = novo_cliente_potencial["Email_Cliente"]  # Já está em minúsculas

        if email_novo in customers_pool:
            customer_data = customers_pool[email_novo]
        else:
            customers_pool[email_novo] = novo_cliente_potencial
            customer_data = novo_cliente_potencial
            cliente_a_ser_inserido_no_bd = novo_cliente_potencial

    sale_date = faker.date_time_between(start_date=start_date, end_date=end_date)
    produto_escolhido_nome = random.choice(list(produtos_dict.keys()))
    produto_info = produtos_dict[produto_escolhido_nome]
    quantidade = random.randint(1, 5)
    total_sale = round(float(produto_info["preco"]) * quantidade, 2)

    dados_venda = (
        sale_id,
        sale_date,
        customer_data["ID_Cliente"],
        produto_info["id"],
        quantidade,
        total_sale,
    )

    dados_cliente_tuple = None
    if cliente_a_ser_inserido_no_bd:
        dados_cliente_tuple = (
            cliente_a_ser_inserido_no_bd["ID_Cliente"],
            cliente_a_ser_inserido_no_bd["Nome_Cliente"],
            cliente_a_ser_inserido_no_bd["Email_Cliente"],
            cliente_a_ser_inserido_no_bd["País"],
        )

    return dados_venda, dados_cliente_tuple


# --- Bloco Principal de Execução ---
# --- Bloco Principal de Execução ---
if __name__ == "__main__":
    if not all(
        [
            DB_CONFIG.get("host"),
            DB_CONFIG.get("user"),
            DB_CONFIG.get("password"),
            DB_CONFIG.get("database"),
        ]
    ):
        print("Erro: Credenciais do banco de dados incompletas no arquivo .env.")
        exit()

    conn = None
    try:
        print("Conectando ao banco de dados MySQL...")
        conn = mysql.connector.connect(**DB_CONFIG)
        cursor = conn.cursor()
        print("Conexão bem-sucedida.")

        produtos_banco = popular_produtos(cursor)
        conn.commit()

        customers_pool = carregar_clientes_existentes(cursor)

        num_records_input = input("Insira a quantidade de VENDAS que deseja criar: ")
        num_records = (
            int(num_records_input.strip()) if num_records_input.strip() else 1000
        )
        year_start = int(
            input("Insira o ano de início para as vendas (ex: 2022): ") or "2022"
        )
        year_end = int(
            input("Insira o ano de fim para as vendas (ex: 2024): ") or "2024"
        )
        start_date = datetime(year_start, 1, 1)
        end_date = datetime(year_end, 12, 31)

        lote_vendas = []
        lote_clientes = []

        print(f"Gerando {num_records} novos registros de vendas...")

        for i in range(1, num_records + 1):
            venda, cliente = generate_sale_data(
                i, start_date, end_date, customers_pool, produtos_banco
            )
            lote_vendas.append(venda)
            if cliente:
                lote_clientes.append(cliente)

            # Processar lotes menores (ex.: a cada 100 registros)
            if i % 100 == 0 or i == num_records:
                print(f"Processando lote... ({i}/{num_records})")
                if lote_clientes:
                    print(f"--> Inserindo {len(lote_clientes)} novos clientes...")
                    cursor.executemany(
                        "INSERT IGNORE INTO Clientes (id_cliente, nome, email, pais) VALUES (%s, %s, %s, %s)",
                        lote_clientes,
                    )
                    conn.commit()  # Commit dos clientes antes das vendas
                    # Atualizar customers_pool com IDs do banco
                    customers_pool = carregar_clientes_existentes(cursor)

                # --- INÍCIO: Verificação de id_cliente ---
                if lote_vendas:
                    id_clientes = [v[2] for v in lote_vendas]
                if id_clientes:  # Verifica se a lista não está vazia
                    placeholders = ",".join(["%s"] * len(id_clientes))
                    cursor.execute(
                        f"SELECT id_cliente FROM Clientes WHERE id_cliente IN ({placeholders})",
                        id_clientes,
                    )
                    clientes_existentes = {row[0] for row in cursor.fetchall()}
                for venda in lote_vendas:
                    if venda[2] not in clientes_existentes:
                        print(
                            f"Erro: id_cliente {venda[2]} não encontrado na tabela Clientes!"
                        )
                else:
                    print("Nenhum id_cliente para verificar no lote atual.")
                    # --- FIM: Verificação de id_cliente ---

                    print(f"--> Inserindo {len(lote_vendas)} novas vendas...")
                    cursor.executemany(
                        "INSERT INTO Vendas (id_venda, data_venda, id_cliente, id_produto, quantidade, total_venda) VALUES (%s, %s, %s, %s, %s, %s)",
                        lote_vendas,
                    )
                    conn.commit()  # Commit das vendas
                    print("--> Lote salvo no banco de dados.")
                    lote_vendas, lote_clientes = [], []

        print("\nProcesso concluído com sucesso!")
        print(
            f"{num_records} vendas foram processadas. O pool agora contém {len(customers_pool)} clientes únicos."
        )

    except mysql.connector.Error as err:
        print(f"Erro de banco de dados: {err}")
    finally:
        if conn and conn.is_connected():
            cursor.close()
            conn.close()
            print("Conexão com o MySQL foi fechada.")
