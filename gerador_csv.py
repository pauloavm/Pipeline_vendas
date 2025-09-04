import csv
import random
from datetime import datetime, timedelta
from faker import Faker
import os
import hashlib

# --- INÍCIO: Configuração Inicial ---
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


def checar_e_renomear_arquivo_existente(caminho_arquivo):
    """
    Verifica se um arquivo existe e o renomeia com um timestamp.
    """
    if os.path.exists(caminho_arquivo):
        mod_time = os.path.getmtime(caminho_arquivo)
        timestamp_str = datetime.fromtimestamp(mod_time).strftime("%Y%m%d_%H%M%S")
        diretorio, nome_arquivo = os.path.split(caminho_arquivo)
        novo_nome_arquivo = f"{timestamp_str}_{nome_arquivo}"
        novo_caminho = os.path.join(diretorio, novo_nome_arquivo)
        os.rename(caminho_arquivo, novo_caminho)
        print(
            f"Arquivo existente '{caminho_arquivo}' foi renomeado para '{novo_caminho}'."
        )


# --- INÍCIO: NOVA FUNÇÃO ---
def preservar_e_renomear_clientes(clientes_path, vendas_path):
    """
    Lê o arquivo de clientes existente, seleciona uma amostra para preservar,
    renomeia os arquivos antigos de clientes e vendas, e retorna a amostra.
    """
    clientes_preservados = []
    if os.path.exists(clientes_path):
        print(
            f"Arquivo '{clientes_path}' encontrado. Preservando uma amostra de clientes..."
        )
        with open(clientes_path, "r", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            todos_clientes = list(reader)

        if todos_clientes:
            percentual = random.choice([0.02, 0.06, 0.09])
            num_a_preservar = int(len(todos_clientes) * percentual)
            clientes_preservados = random.sample(todos_clientes, num_a_preservar)
            print(
                f"--> Preservando {len(clientes_preservados)} clientes ({percentual:.0%}) da base antiga."
            )

    # Renomeia os arquivos antigos DEPOIS de ler os dados
    checar_e_renomear_arquivo_existente(clientes_path)
    checar_e_renomear_arquivo_existente(vendas_path)

    return clientes_preservados


# --- FIM: NOVA FUNÇÃO ---


def gerar_produtos_csv(caminho_arquivo="produtos.csv"):
    """Gera o arquivo produtos.csv e retorna um dicionário com os produtos."""
    checar_e_renomear_arquivo_existente(caminho_arquivo)

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


def obter_ultimo_id_e_data(caminho_vendas):
    if not os.path.exists(caminho_vendas):
        return 0, None
    # Esta função agora é menos crítica, pois sempre começamos um novo arquivo de vendas.
    # Mas pode ser mantida para outros propósitos ou lógicas futuras.
    return 0, None


def generate_customer_email(customer_name):
    name_parts = "".join(filter(str.isalnum, customer_name.lower().replace(" ", "")))
    email_domains = ["gmail.com", "outlook.com", "yahoo.com.br", "hotmail.com"]
    return f"{name_parts}@{random.choice(email_domains)}"


def gerar_id_cliente(email):
    hash_object = hashlib.sha256(email.encode("utf-8"))
    id_numerico = int(hash_object.hexdigest()[:15], 16)
    return id_numerico


# --- FUNÇÃO PRINCIPAL DE GERAÇÃO DE VENDAS (MODIFICADA) ---
def gerar_clientes_e_vendas_csv(
    num_vendas,
    start_date,
    end_date,
    produtos_dict,
    clientes_path="clientes.csv",
    vendas_path="vendas.csv",
):
    """
    Gera novos arquivos clientes.csv e vendas.csv.
    Preserva uma amostra aleatória de clientes se o arquivo já existir.
    """
    # 1. Preserva amostra de clientes e renomeia arquivos antigos
    clientes_preservados = preservar_e_renomear_clientes(clientes_path, vendas_path)

    # 2. Prepara o pool de clientes, começando com os preservados
    clientes_pool = {cliente["email"]: cliente for cliente in clientes_preservados}
    id_venda_counter = 1

    print(f"Gerando {num_vendas} novas vendas...")

    # 3. Abre os novos arquivos em modo de escrita ('w')
    with open(clientes_path, "w", newline="", encoding="utf-8") as clientes_file, open(
        vendas_path, "w", newline="", encoding="utf-8"
    ) as vendas_file:

        clientes_fieldnames = [
            "id_cliente",
            "nome",
            "email",
            "pais",
            "estado",
            "cidade",
        ]
        clientes_writer = csv.DictWriter(clientes_file, fieldnames=clientes_fieldnames)

        vendas_fieldnames = [
            "id_venda",
            "data_venda",
            "id_cliente",
            "id_produto",
            "quantidade",
            "total_venda",
        ]
        vendas_writer = csv.DictWriter(vendas_file, fieldnames=vendas_fieldnames)

        # 4. Escreve os cabeçalhos e os clientes preservados
        clientes_writer.writeheader()
        vendas_writer.writeheader()
        if clientes_preservados:
            clientes_writer.writerows(clientes_preservados)

        total_segundos = int((end_date - start_date).total_seconds())
        if total_segundos <= 0:
            print("Erro: intervalo de datas inválido.")
            return

        momentos = sorted(random.sample(range(total_segundos), num_vendas))

        # 5. Loop para gerar novas vendas e clientes
        for offset in momentos:
            data_venda = start_date + timedelta(seconds=offset)

            # Decide se usa um cliente antigo (preservado) ou cria um novo
            if (
                clientes_pool and random.random() < 0.3
            ):  # 30% de chance de ser um cliente recorrente
                email_cliente = random.choice(list(clientes_pool.keys()))
                id_cliente_venda = clientes_pool[email_cliente]["id_cliente"]
            else:
                nome_cliente = faker.name()
                email_cliente = generate_customer_email(nome_cliente).lower()
                id_cliente_fix = gerar_id_cliente(email_cliente)

                if email_cliente not in clientes_pool:
                    novo_cliente = {
                        "id_cliente": id_cliente_fix,
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
                    # Caso raro de colisão de e-mail gerado
                    id_cliente_venda = clientes_pool[email_cliente]["id_cliente"]

            produto_escolhido_nome = random.choice(list(produtos_dict.keys()))
            produto_info = produtos_dict[produto_escolhido_nome]
            quantidade = random.randint(1, 3)

            venda = {
                "id_venda": id_venda_counter,
                "data_venda": data_venda.strftime("%Y-%m-%d %H:%M:%S"),
                "id_cliente": id_cliente_venda,
                "id_produto": produto_info["id"],
                "quantidade": quantidade,
                "total_venda": round(float(produto_info["preco"]) * quantidade, 2),
            }
            vendas_writer.writerow(venda)
            id_venda_counter += 1

            if id_venda_counter % 100 == 0:
                print(f"--> {id_venda_counter}/{num_vendas} vendas geradas...")

    print(f"Novos arquivos {clientes_path} e {vendas_path} gerados com sucesso.")
    print(f"Total de {len(clientes_pool)} clientes únicos registrados.")


# --- Bloco Principal de Execução (sem alterações) ---
if __name__ == "__main__":
    if not os.path.exists("data"):
        os.makedirs("data")

    produtos_info = gerar_produtos_csv("data/produtos.csv")

    num_records_input = input(
        "Insira a quantidade de NOVAS VENDAS a criar (padrão: 1000): "
    )
    num_records = int(num_records_input.strip()) if num_records_input.strip() else 1000

    year_start_input = input("Insira o ano de início para as vendas (padrão: 2022): ")
    year_start = int(year_start_input) if year_start_input.strip() else 2022

    year_end_input = input("Insira o ano de fim para as vendas (padrão: 2024): ")
    year_end = int(year_end_input) if year_end_input.strip() else 2024

    start_date = datetime(year_start, 1, 1)
    end_date = datetime(year_end, 12, 31, 23, 59, 59)

    gerar_clientes_e_vendas_csv(
        num_vendas=num_records,
        start_date=start_date,
        end_date=end_date,
        produtos_dict=produtos_info,
        clientes_path="data/clientes.csv",
        vendas_path="data/vendas.csv",
    )

    print("\nProcesso de geração de dados concluído com sucesso!")
    print("Os ficheiros CSV foram atualizados na pasta 'data'.")