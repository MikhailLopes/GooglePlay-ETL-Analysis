import os
import logging
import pandas as pd
from tabulate import tabulate
from sqlalchemy import create_engine

# Configura o logging para todo o pipeline
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Conceito: As instruções from ... import ... são como você "pega emprestado" as funções que você definiu em outros arquivos (.py) para usá-las aqui.
# Ipomrtações extract_data do seu módulo de extração.
from extract.extract_csv import extract_data
from transform.transform import transform_google_play_apps, transform_user_reviews, aggregate_reviews, unify_dataframes
from load.load_SQLite import load_dataframe_to_db


# Parâmetros de Entrada: A função recebe os caminhos dos arquivos (apps_file_path, reviews_file_path) e o diretório de saída (output_dir).
def run_etl_pipeline(apps_file_path: str, reviews_file_path: str, output_dir: str):
    logging.info("Iniciando o pipeline ETL...")


    # --- 1. Extração ---

    # Chamada extract_data
        # df_apps_raw, df_reviews_raw = extract_data(apps_file_path, reviews_file_path): Aqui, o main.py chama a função do módulo extract para obter os DataFrames brutos
    logging.info("Passo 1: Extraindo dados brutos...")
    df_apps_raw, df_reviews_raw = extract_data(apps_file_path, reviews_file_path)

    # Conceito: Esta é uma validação de "early exit"
        # Se a extração falhar (e extract_data retornar None, None), o pipeline é interrompido aqui, evitando que as próximas etapas tentem processar dados inexistentes e gerem mais erros.
    if df_apps_raw is None or df_reviews_raw is None:
        logging.error("Falha na extração dos dados.")
        return


    # --- 2. Transformação ---

    # Conceito: Novamente, o main.py orquestra as chamadas, passando o resultado de uma etapa para a próxima
        # Chamadas de Transformação:
            # df_apps_transformed = transform_google_play_apps(df_apps_raw): O main.py chama a função de transformação para os dados de aplicativos.
            # df_reviews_transformed = transform_user_reviews(df_reviews_raw): E, em seguida, para os dados de reviews.

    logging.info("Passo 2: Transformando dados...")
    df_apps_transformed = transform_google_play_apps(df_apps_raw)
    df_reviews_transformed = transform_user_reviews(df_reviews_raw)

    if df_apps_transformed is None or df_reviews_transformed is None:
        logging.error("Falha na transformação dos dados. Encerrando o pipeline ETL.")
        return

    logging.info(f"Transformação de apps e reviews concluída.")

    # Conceito: Resolver a granularidade aqui é fundamental para a qualidade da análise no Power BI.
        # Agregação de Reviews:
            # df_reviews_aggregated = aggregate_reviews(df_reviews_transformed): 
            # Esta linha é crucial. O main.py chama a função aggregate_reviews (do módulo transform) para resumir os dados de reviews antes de qualquer unificação ou carga final.

    df_reviews_aggregated = aggregate_reviews(df_reviews_transformed)


    # --- 3. Carga no banco de dados SQLite ---
    
    sqlite_db_path = os.path.join(output_dir, 'googleplay_data_silver.sqlite')
    sqlite_conn_string = f"sqlite:///{sqlite_db_path}"

    logging.info("Passo 3: Carregando dados transformados para o banco de dados (SQLite)...")
    
    # Chamada para carregar a tabela de apps
    apps_table_name = "googleplaystore_apps_silver"
    load_dataframe_to_db(df_apps_transformed, sqlite_conn_string, apps_table_name)
    
    # Chamada para carregar a tabela de reviews
    reviews_table_name = "googleplaystore_user_reviews_silver"
    load_dataframe_to_db(df_reviews_aggregated, sqlite_conn_string, reviews_table_name)
    
     # Chamada para carregar a tabela unificada
    logging.info("Carregando tabela unificada...")
    df_unified = unify_dataframes(df_apps_transformed, df_reviews_aggregated)

    if df_unified is not None:
        load_dataframe_to_db(df_unified, sqlite_conn_string, 'googleplay_data')
    else:
        logging.error("Falha ao unificar os DataFrames. A tabela unificada não será criada.")
    
    logging.info("Pipeline ETL concluído com sucesso!")


# Então, if __name__ == "__main__": literalmente se traduz como: 
    # "Se este script for o que está sendo executado diretamente, então execute o código indentado abaixo desta linha.
    # Em essência, if __name__ == "__main__": é uma forma padrão e poderosa de estruturar aplicações Python, tornando-as tanto executáveis quanto reutilizáveis.


# Por que o Utilizamos:

    # Distinguir Execução Direta de Importação: Permite escrever código que serve a um propósito duplo:
    # Como um programa executável: Quando você executa o script diretamente, o código dentro de if __name__ == "__main__": é executado, tipicamente configurando e iniciando a lógica principal da sua aplicação.
    # Como um módulo reutilizável: Quando outros scripts importam o seu script, o código dentro deste bloco é ignorado. Isso significa que funções e classes definidas no seu script ficam disponíveis para importação e uso por outros scripts sem executar automaticamente a lógica principal do programa.
    # Prevenir Efeitos Colaterais Indesejados: Sem essa construção, se você definisse e chamasse funções diretamente no escopo global do seu script, essas funções seriam executadas toda vez que o script fosse importado por outra parte do seu projeto. Isso poderia levar a ações indesejadas, como executar um pipeline ETL quando você só queria importar uma única função de transformação.

# if __name__ == "__main__", desempenha papel vital: 
    # 1. Orquestração: chamada da função run_etl_pipeline() e a configuração de apps_csv_path, reviews_csv_path e output_folder são colocadas dentro deste bloco
    # 2. Ponto de Entrada Claro: define claramente o ponto de entrada do seu processo ETL. Quando você executa python main.py, o código dentro deste bloco é executado, iniciando toda a sequência de extração, transformação e carregamento de dados.
    # 3. Modularidade: Se outro script (talvez um futuro dashboard ou um verificador de qualidade de dados) quisesse importar uma função específica dos seus módulos extract.py ou transform.py (por exemplo, from transform.transform import transform_google_play_apps), ele poderia fazê-lo sem executar automaticamente todo o pipeline ETL definido em main.py. Isso mantém seu projeto limpo e evita execuções desnecessárias.


if __name__ == "__main__":
    # --- Configurações ---
    apps_csv_path = r'C:\Users\HP\Documents\projects\teste_tecnico\googleplaystore.csv'
    reviews_csv_path = r'C:\Users\HP\Documents\projects\teste_tecnico\googleplaystore_user_reviews.csv'
    output_folder = r'C:\Users\HP\Documents\projects\teste_tecnico\output'

    if not os.path.exists(output_folder):
        os.makedirs(output_folder)
        logging.info(f"Diretório de saída '{output_folder}' criado.")

    run_etl_pipeline(
        apps_file_path=apps_csv_path,
        reviews_file_path=reviews_csv_path,
        output_dir=output_folder,
    )
