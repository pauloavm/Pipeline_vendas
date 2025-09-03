import csv
import random
from datetime import datetime
from faker import Faker
import os

# --- INÍCIO: Configuração Inicial ---
# Foco exclusivo no Brasil
faker = Faker("pt_BR")

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
# --- FIM: Configuração Inicial ---


def gerar_produtos_csv(caminho_arquivo="produtos.csv"):
    """Gera o arquivo produtos.csv e retorna um dicionário com os produtos."""
    print("Gerando arquivo produtos.csv...")
    produtos_dict = {}
    id_produto_counter = 1
    with open(caminho_arquivo, "w", newline="", encoding="utf-8") as csvfile:
        fieldnames = ["id_produto", "nome", "categoria", "preco_unitario"]
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
        writer.writeheader()

        for categoria, produtos in produtos_eletronicos.items():
            for nome, preco in produtos.items():
                linha = {
                    "id_produto": id_produto_counter,
                    "nome": nome,
                    "categoria": categoria,
                    "preco_unitario": preco,
                }
                writer.writerow(linha)
                produtos_dict[nome] = {
                    "id": id_produto_counter,
                    "preco": preco,
                }
                id_produto_counter += 1
    print(f"Arquivo {caminho_arquivo} gerado com sucesso.")
    return produtos_dict


def generate_customer_email(customer_name):
    """Gera um e-mail a partir de um nome de cliente."""
    name_parts = "".join(filter(str.isalnum, customer_name.lower().replace(" ", "")))
    email_domains = ["gmail.com", "outlook.com", "yahoo.com.br", "hotmail.com"]
    return f"{name_parts}@{random.choice(email_domains)}"


def gerar_clientes_e_vendas_csv(
    num_vendas,
    start_date,
    end_date,
    produtos_dict,
    clientes_path="clientes.csv",
    vendas_path="vendas.csv",
):
    """Gera os arquivos clientes.csv e vendas.csv com dados do Brasil."""
    print(f"Gerando {num_vendas} vendas e clientes do Brasil...")

    clientes_pool = {}

    with open(clientes_path, "w", newline="", encoding="utf-8") as clientes_file, open(
        vendas_path, "w", newline="", encoding="utf-8"
    ) as vendas_file:

        # Configuração do CSV de Clientes com os novos campos
        clientes_fieldnames = [
            "id_cliente",
            "nome",
            "email",
            "pais",
            "estado",
            "cidade",
        ]
        clientes_writer = csv.DictWriter(clientes_file, fieldnames=clientes_fieldnames)
        clientes_writer.writeheader()

        # Configuração do CSV de Vendas
        vendas_fieldnames = [
            "id_venda",
            "data_venda",
            "id_cliente",
            "id_produto",
            "quantidade",
            "total_venda",
        ]
        vendas_writer = csv.DictWriter(vendas_file, fieldnames=vendas_fieldnames)
        vendas_writer.writeheader()

        for i in range(1, num_vendas + 1):
            id_cliente_venda = None

            if random.random() < 0.7 and clientes_pool:
                id_cliente_venda = random.choice(list(clientes_pool.values()))[
                    "id_cliente"
                ]
            else:
                nome_cliente = faker.name()
                email_cliente = generate_customer_email(nome_cliente).lower()

                if email_cliente not in clientes_pool:
                    novo_cliente = {
                        "id_cliente": str(faker.uuid4()),
                        "nome": nome_cliente,
                        "email": email_cliente,
                        "pais": "Brasil",
                        "estado": faker.state(),
                        "cidade": faker.city(),
                    }
                    clientes_pool[email_cliente] = novo_cliente
                    clientes_writer.writerow(novo_cliente)
                    id_cliente_venda = novo_cliente["id_cliente"]
                else:
                    id_cliente_venda = clientes_pool[email_cliente]["id_cliente"]

            produto_escolhido_nome = random.choice(list(produtos_dict.keys()))
            produto_info = produtos_dict[produto_escolhido_nome]
            quantidade = random.randint(1, 5)

            venda = {
                "id_venda": i,
                "data_venda": faker.date_time_between(
                    start_date=start_date, end_date=end_date
                ),
                "id_cliente": id_cliente_venda,
                "id_produto": produto_info["id"],
                "quantidade": quantidade,
                "total_venda": round(float(produto_info["preco"]) * quantidade, 2),
            }
            vendas_writer.writerow(venda)

            if i % 100 == 0:
                print(f"--> {i}/{num_vendas} vendas geradas...")

    print(f"Arquivo {clientes_path} e {vendas_path} gerados com sucesso.")
    print(f"Total de {len(clientes_pool)} clientes únicos criados.")


# --- Bloco Principal de Execução ---
if __name__ == "__main__":
    if not os.path.exists("data"):
        os.makedirs("data")

    produtos_info = gerar_produtos_csv("data/produtos.csv")

    num_records_input = input(
        "Insira a quantidade de VENDAS que deseja criar (padrão: 1000): "
    )
    num_records = int(num_records_input.strip()) if num_records_input.strip() else 1000

    year_start_input = input("Insira o ano de início para as vendas (padrão: 2022): ")
    year_start = int(year_start_input) if year_start_input.strip() else 2022

    year_end_input = input("Insira o ano de fim para as vendas (padrão: 2024): ")
    year_end = int(year_end_input) if year_end_input.strip() else 2024

    start_date = datetime(year_start, 1, 1)
    end_date = datetime(year_end, 12, 31)

    gerar_clientes_e_vendas_csv(
        num_vendas=num_records,
        start_date=start_date,
        end_date=end_date,
        produtos_dict=produtos_info,
        clientes_path="data/clientes.csv",
        vendas_path="data/vendas.csv",
    )

    print("\nProcesso de geração de dados concluído com sucesso!")
    print("Os ficheiros CSV foram guardados na pasta 'data'.")
