import pandas as pd
import logging


def params_csv(file_path: str, delimiter: str = ",", encoding: str = "utf-8", quotechar: str = '"', engine: str = "python", **kwargs ) -> pd.DataFrame:
    logging.info(f"Tentando ler o arquivo CSV: {file_path}")
    
    return pd.read_csv(file_path, delimiter=delimiter, encoding=encoding, quotechar=quotechar, engine=engine, **kwargs)


def extract_data(file_path_apps: str, file_path_reviews: str) -> tuple[pd.DataFrame, pd.DataFrame]:

    df_apps = None
    df_reviews = None

    try:
        logging.info(f"Iniciando extração de dados: {file_path_apps}")
        df_apps = params_csv(file_path_apps)
        logging.info("Google Play Store (apps) extraídos com sucesso.")

        logging.info(f"Iniciando extração de dados: {file_path_reviews}")
        df_reviews = params_csv(file_path_reviews)
        logging.info("User Reviews extraídos com sucesso.")

        return df_apps, df_reviews
    
    except FileNotFoundError as e:
        logging.error(f"Erro: Arquivos não foram encontrados. Detalhes: {e}")

        return None, None
    
    except Exception as e:
        logging.error(f"Erro durante a extração dos dados: {e}")

        return None, None
