import requests
import json
import os

api_key = "" # Chave da API do TMDB

# Retorna uma lista de ids dos filmes escolhidos
def obter_dados():
    # Crio uma lista para armazenar os ids dos filmes escolhidos para depois fazer uma busca mais detalhada
    ids_filmes = []
    pagina = 1
    # O While serve principalmente para percorrer todas as páginas do resultado
    while True:#                                                                                                 878=ficção cintífica         with_keywords=nave espacial|space|moon|moon base|astronaut|space travel|space mission|spaceman       without_keywords=super hero          without_companies=marvel
        url = f"https://api.themoviedb.org/3/discover/movie?api_key={api_key}&language=pt-BR&page={pagina}&with_genres=878&sort_by=popularity.desc&with_keywords=234007|9882|305|12185|14626|3801|4040|1432|2345&without_keywords=318450&without_companies=420|574|5822"

        response = requests.get(url)

        # Transforma o resultado do get em um JSON
        data = response.json()

        if pagina > data["total_pages"]:
            break

        # Adiciona o id de cada filme em um Array
        for filme in data["results"]:
            ids_filmes.append(filme["id"])

        #Incrementa a página
        pagina += 1
    return ids_filmes

# Método que serve para dividir a lista de ids em listas com no máximo 100 ids
def dividir_arquivo(ids_filmes:list):
    #Define o limite de registros por arquivo
    limite = 100

    # Divide o "ids_filmes" em listas, fazendo com uqe cada lista não ultrapasse o limite de registros(100)
    arquivos = [ids_filmes[i: i+limite] for i in range(0, len(ids_filmes), limite)]
    return arquivos

# Método que serve para formatar o arquivo com as informações do meu interesse, e no final salva o arquivo
def formatando_salvando_dados(lista_ids:list):
    # Percorre todas as listas de ids que estão em "lista_ids"
    for indice, ids in enumerate(lista_ids, 1):
        filmes = [] # Array vazio para armazenar os dados dos filmes
        # Passa por cada id dentro da lista
        for id in ids:
            # Obter os dados dos filmes
            url = f"https://api.themoviedb.org/3/movie/{id}?api_key={api_key}&language=pt-BR"

            response = requests.get(url)

            # Transforma o resultado em JSON
            data = response.json()

            # Formata o arquivo data para melhor Manipulação e Visualização
            df = {
                "titulo": data["title"],
                "titulo_original": data["original_title"],
                "visao_geral": data["overview"],
                "generos": [genero["name"] for genero in data["genres"]],
                "tempo_duracao(minutos)": data["runtime"],
                "id_filme": data["id"],
                "id_imdb": data["imdb_id"],
                "media_de_votos": data["vote_average"],
                "total_votos": data["vote_count"],
                "popularidade": data["popularity"],
                "receita": data["revenue"],
                "produtoras": [produtora["name"] for produtora in data["production_companies"]],
            }

            # Adiciona o JSON acima na lista filmes
            filmes.append(df)
        
        # Definindo nome do arquivo
        nome_arquivo = f"filmes({indice}).json"

        # Converter a lista de filmes para JSON
        arquivo_json = json.dumps(filmes, ensure_ascii=False, indent=2)

        # Adiciona o arquivo na pasta "json"
        adicionar_arquivo(nome_arquivo, arquivo_json)

        # Adicionar arquivos em um diretório
        print(f"Arquivo {nome_arquivo} Upado com Sucesso")

    
def adicionar_arquivo(nome_arquivo, arquivo):
    # Caminho do diretório de destino
    diretorio_destino = "extracao\json"

    # Path completo do arquivo de destino
    path_destino = os.path.join(diretorio_destino, nome_arquivo)

    # Escreve o arquivo no diretório de destino
    with open(path_destino, 'w', encoding="utf-8") as f:
        f.write(arquivo)

#           MAIN
ids = obter_dados()
lista_ids = dividir_arquivo(ids)
formatando_salvando_dados(lista_ids)
