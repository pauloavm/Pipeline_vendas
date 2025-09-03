from faker import Faker
import random
from datetime import datetime
import mysql.connector
from dotenv import load_dotenv  # Importa a função para carregar o .env
import os  # Importa a biblioteca para acessar as variáveis de ambiente

# --- INÍCIO: Carregamento das Credenciais ---
# Carrega as variáveis do arquivo .env para o ambiente da aplicação
load_dotenv()

# Busca as credenciais do ambiente e monta o dicionário de configuração
# A função os.getenv('CHAVE') lê a variável do ambiente.
DB_CONFIG = {
    "host": os.getenv("DB_HOST"),
    "port": os.getenv("DB_PORT"),
    "user": os.getenv("DB_USER"),
    "password": os.getenv("DB_PASSWORD"),
    "database": os.getenv("DB_NAME"),
}
# --- FIM: Carregamento das Credenciais ---

# O restante do código permanece exatamente o mesmo
# ... (código de inicialização do Faker, dicionário de produtos, funções, etc.) ...
# Inicializando a Faker
locales = ["en_US", "pt_BR", "es_ES", "fr_FR", "de_DE", "it_IT", "ru_RU", "pt_PT"]
faker = Faker(locales)

# Dicionário de produtos
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


def popular_produtos(cursor):
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


def generate_customer_email(customer_name, existing_emails):
    name_parts = customer_name.lower().split()
    first_name = name_parts[0]
    last_name = name_parts[-1] if len(name_parts) > 1 else ""
    email_domains = ["gmail.com", "outlook.com", "yahoo.com", "hotmail.com"]
    base_email = f"{first_name}.{last_name}".replace(" ", "")
    email_domain = random.choice(email_domains)
    customer_email = f"{base_email}@{email_domain}"
    counter = 1
    while customer_email in existing_emails:
        customer_email = f"{base_email}{counter}@{email_domain}"
        counter += 1
    return customer_email


def generate_customer(existing_customers_emails):
    locale = random.choice(locales)
    faker_locale = Faker(locale)
    customer_name = faker_locale.name()
    customer_email = generate_customer_email(customer_name, existing_customers_emails)
    customer_country = faker_locale.country()
    return {
        "ID_Cliente": faker.uuid4(),
        "Nome_Cliente": customer_name,
        "Email_Cliente": customer_email,
        "País": customer_country,
    }


def generate_sale_data(sale_id, start_date, end_date, customers_pool, produtos_dict):
    cliente_novo = None

    if random.random() < 0.7 and customers_pool:
        customer = random.choice(list(customers_pool.values()))
    else:
        existing_emails = [c["Email_Cliente"] for c in customers_pool.values()]
        new_customer = generate_customer(existing_emails)
        customers_pool[new_customer["ID_Cliente"]] = new_customer
        customer = new_customer
        cliente_novo = new_customer

    sale_date = faker.date_time_between(start_date=start_date, end_date=end_date)

    produto_escolhido_nome = random.choice(list(produtos_dict.keys()))
    produto_info = produtos_dict[produto_escolhido_nome]

    id_produto = produto_info["id"]
    product_price = produto_info["preco"]
    quantity = random.randint(1, 5)
    total_sale = round(float(product_price) * quantity, 2)

    dados_venda = (
        sale_id,
        sale_date,
        customer["ID_Cliente"],
        id_produto,
        quantity,
        total_sale,
    )

    dados_cliente = None
    if cliente_novo:
        dados_cliente = (
            cliente_novo["ID_Cliente"],
            cliente_novo["Nome_Cliente"],
            cliente_novo["Email_Cliente"],
            cliente_novo["País"],
        )

    return dados_venda, dados_cliente


if __name__ == "__main__":
    # Verificação simples para garantir que as credenciais foram carregadas
    if not all(DB_CONFIG.values()):
        print(
            "Erro: As credenciais do banco de dados não foram encontradas no arquivo .env ou estão incompletas."
        )
        exit()

    try:
        print("Conectando ao banco de dados MySQL...")
        conn = mysql.connector.connect(**DB_CONFIG)
        cursor = conn.cursor()
        print("Conexão bem-sucedida.")

        produtos_banco = popular_produtos(cursor)
        conn.commit()

        num_records_input = input("Insira a quantidade de VENDAS que deseja criar: ")
        num_records = int(num_records_input) if num_records_input.strip() else 1000
        year_start = int(
            input("Insira o ano de início para as vendas (ex: 2022): ") or "2022"
        )
        year_end = int(
            input("Insira o ano de fim para as vendas (ex: 2024): ") or "2024"
        )
        start_date = datetime(year_start, 1, 1)
        end_date = datetime(year_end, 12, 31)

        customers_pool = {}
        lote_vendas = []
        lote_clientes = []

        print(f"Gerando {num_records} registros de vendas...")

        for i in range(1, num_records + 1):
            venda, cliente = generate_sale_data(
                i, start_date, end_date, customers_pool, produtos_banco
            )

            lote_vendas.append(venda)
            if cliente:
                lote_clientes.append(cliente)

            if i % 1000 == 0:
                print(
                    f"Inserindo lote de {len(lote_clientes)} clientes e {len(lote_vendas)} vendas... ({i}/{num_records})"
                )
                if lote_clientes:
                    cursor.executemany(
                        "INSERT IGNORE INTO Clientes (id_cliente, nome, email, pais) VALUES (%s, %s, %s, %s)",
                        lote_clientes,
                    )
                cursor.executemany(
                    "INSERT INTO Vendas (id_venda, data_venda, id_cliente, id_produto, quantidade, total_venda) VALUES (%s, %s, %s, %s, %s, %s)",
                    lote_vendas,
                )
                conn.commit()
                lote_vendas = []
                lote_clientes = []

        if lote_vendas:
            print(
                f"Inserindo lote final de {len(lote_clientes)} clientes e {len(lote_vendas)} vendas..."
            )
            if lote_clientes:
                cursor.executemany(
                    "INSERT IGNORE INTO Clientes (id_cliente, nome, email, pais) VALUES (%s, %s, %s, %s)",
                    lote_clientes,
                )
            cursor.executemany(
                "INSERT INTO Vendas (id_venda, data_venda, id_cliente, id_produto, quantidade, total_venda) VALUES (%s, %s, %s, %s, %s, %s)",
                lote_vendas,
            )
            conn.commit()

        print("\nProcesso concluído com sucesso!")
        print(
            f"{num_records} vendas e {len(customers_pool)} clientes foram processados."
        )

    except mysql.connector.Error as err:
        print(f"Erro de banco de dados: {err}")
    finally:
        if "conn" in locals() and conn.is_connected():
            cursor.close()
            conn.close()
            print("Conexão com o MySQL foi fechada.")
