import pandas as pd
from faker import Faker
import random
from datetime import datetime
import mysql.connector # Importa a biblioteca de conexão

# --- INÍCIO: Configuração da Conexão com o Banco de Dados ---
# ATENÇÃO: Substitua pelos seus dados de conexão do MySQL
DB_CONFIG = {
    'host': '192.168.0.20:3306',  # Ex: 'localhost' ou '
    'user': 'seu_usuario',      # Ex: 'root'
    'password': 'sua_senha',    # Ex: '123456'
    'database': 'xingcel'
}
# --- FIM: Configuração da Conexão ---


# Inicializando a Faker (sem alterações)
locales = ["en_US", "pt_BR", "es_ES", "fr_FR", "de_DE", "it_IT", "ru_RU", "pt_PT"]
faker = Faker(locales)

# Dicionário de produtos (sem alterações)
produtos_eletronicos = {
    "Celulares": { "iPhone 13": 850.00, "Samsung Galaxy S22": 799.00, "Google Pixel 6": 699.00, "Xiaomi 12": 599.00, "OnePlus 10 Pro": 750.00 },
    "Acessórios": { "Carregador USB-C": 25.00, "Capa de Silicone": 15.00, "Fone de Ouvido Bluetooth": 50.00, "Smartwatch": 150.00, "Power Bank 10000mAh": 30.00, "Protetor de tela de vidro": 10.00 },
}

# --- NOVA FUNÇÃO: Popular a tabela de produtos ---
def popular_produtos(cursor):
    """Insere os produtos do dicionário na tabela Produtos e retorna um dict com os dados do BD."""
    print("Populando a tabela de Produtos...")
    lista_produtos_para_inserir = []
    for categoria, produtos in produtos_eletronicos.items():
        for nome, preco in produtos.items():
            lista_produtos_para_inserir.append((nome, categoria, preco))

    # Usamos INSERT IGNORE para não dar erro se o produto já existir (baseado na chave UNIQUE 'nome')
    sql = "INSERT IGNORE INTO Produtos (nome, categoria, preco_unitario) VALUES (%s, %s, %s)"
    cursor.executemany(sql, lista_produtos_para_inserir)
    
    # Após inserir, vamos buscar todos os produtos para ter acesso aos seus IDs
    cursor.execute("SELECT id_produto, nome, preco_unitario FROM Produtos")
    produtos_do_banco = cursor.fetchall()
    
    # Transforma a lista de produtos em um dicionário para acesso rápido
    # Ex: {'iPhone 13': {'id': 1, 'preco': 850.00}, ...}
    produtos_dict = {nome: {'id': id_prod, 'preco': preco} for id_prod, nome, preco in produtos_do_banco}
    print("Tabela de Produtos populada e carregada.")
    return produtos_dict

# Funções generate_customer_email e generate_customer (sem alterações)
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
        "ID_Cliente": faker.uuid4(), "Nome_Cliente": customer_name,
        "Email_Cliente": customer_email, "País": customer_country,
    }

# --- FUNÇÃO PRINCIPAL MODIFICADA ---
def generate_sale_data(sale_id, start_date, end_date, customers_pool, produtos_dict):
    """Gera os dados de um cliente e de uma venda para serem inseridos no banco."""
    
    cliente_novo = None # Para saber se precisamos inserir um novo cliente
    
    # Lógica para escolher um cliente existente ou criar um novo
    if random.random() < 0.7 and customers_pool:
        customer = random.choice(list(customers_pool.values()))
    else:
        # Pega todos os emails já em uso para garantir a criação de um email único
        existing_emails = [c['Email_Cliente'] for c in customers_pool.values()]
        new_customer = generate_customer(existing_emails)
        
        # Adiciona o novo cliente ao pool para poder ser reutilizado nesta execução
        customers_pool[new_customer['ID_Cliente']] = new_customer
        customer = new_customer
        cliente_novo = new_customer

    sale_date = faker.date_time_between(start_date=start_date, end_date=end_date)
    
    # Seleciona um produto aleatório do nosso dicionário de produtos do banco
    produto_escolhido_nome = random.choice(list(produtos_dict.keys()))
    produto_info = produtos_dict[produto_escolhido_nome]

    id_produto = produto_info['id']
    product_price = produto_info['preco']
    quantity = random.randint(1, 5)
    total_sale = round(float(product_price) * quantity, 2)

    # Prepara os dados da venda para inserção
    dados_venda = (
        sale_id,
        sale_date,
        customer["ID_Cliente"],
        id_produto,
        quantity,
        total_sale,
    )

    # Se um novo cliente foi criado, prepara seus dados também
    dados_cliente = None
    if cliente_novo:
        dados_cliente = (
            cliente_novo['ID_Cliente'],
            cliente_novo['Nome_Cliente'],
            cliente_novo['Email_Cliente'],
            cliente_novo['País']
        )
        
    return dados_venda, dados_cliente


# --- BLOCO PRINCIPAL DE EXECUÇÃO ---
if __name__ == "__main__":
    try:
        # Conectando ao banco de dados
        print("Conectando ao banco de dados MySQL...")
        conn = mysql.connector.connect(**DB_CONFIG)
        cursor = conn.cursor()
        print("Conexão bem-sucedida.")

        # 1. Popula a tabela de produtos e carrega os dados para o script
        produtos_banco = popular_produtos(cursor)
        conn.commit()

        # 2. Coleta de informações do usuário (para quantidade e datas)
        num_records_input = input("Insira a quantidade de VENDAS que deseja criar: ")
        num_records = int(num_records_input) if num_records_input.strip() else 1000
        year_start = int(input("Insira o ano de início para as vendas (ex: 2022): ") or "2022")
        year_end = int(input("Insira o ano de fim para as vendas (ex: 2024): ") or "2024")
        start_date = datetime(year_start, 1, 1)
        end_date = datetime(year_end, 12, 31)

        # 3. Geração dos dados em lotes
        customers_pool = {} # Usaremos um dicionário para acesso rápido pelo ID
        lote_vendas = []
        lote_clientes = []
        
        print(f"Gerando {num_records} registros de vendas...")
        
        for i in range(1, num_records + 1):
            venda, cliente = generate_sale_data(i, start_date, end_date, customers_pool, produtos_banco)
            
            lote_vendas.append(venda)
            if cliente:
                lote_clientes.append(cliente)

            # Insere os lotes no banco a cada 1000 registros para otimizar
            if i % 1000 == 0:
                print(f"Inserindo lote de {len(lote_clientes)} clientes e {len(lote_vendas)} vendas... ({i}/{num_records})")
                if lote_clientes:
                    cursor.executemany("INSERT IGNORE INTO Clientes (id_cliente, nome, email, pais) VALUES (%s, %s, %s, %s)", lote_clientes)
                cursor.executemany("INSERT INTO Vendas (id_venda, data_venda, id_cliente, id_produto, quantidade, total_venda) VALUES (%s, %s, %s, %s, %s, %s)", lote_vendas)
                conn.commit()
                # Limpa os lotes após a inserção
                lote_vendas = []
                lote_clientes = []

        # Insere o lote final, caso o total não seja múltiplo de 1000
        if lote_vendas:
            print(f"Inserindo lote final de {len(lote_clientes)} clientes e {len(lote_vendas)} vendas...")
            if lote_clientes:
                cursor.executemany("INSERT IGNORE INTO Clientes (id_cliente, nome, email, pais) VALUES (%s, %s, %s, %s)", lote_clientes)
            cursor.executemany("INSERT INTO Vendas (id_venda, data_venda, id_cliente, id_produto, quantidade, total_venda) VALUES (%s, %s, %s, %s, %s, %s)", lote_vendas)
            conn.commit()

        print("\nProcesso concluído com sucesso!")
        print(f"{num_records} vendas e {len(customers_pool)} clientes foram processados.")

    except mysql.connector.Error as err:
        print(f"Erro de banco de dados: {err}")
    finally:
        # Garante que a conexão seja fechada no final
        if 'conn' in locals() and conn.is_connected():
            cursor.close()
            conn.close()
            print("Conexão com o MySQL foi fechada.")