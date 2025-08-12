import pandas as pd
from sqlalchemy import create_engine
import logging


def load_dataframe_to_db(df: pd.DataFrame, connection_string: str, table_name: str, if_exists: str = 'replace') -> None:

    if df.empty:
        logging.warning("DataFrame vazio recebido para carga, portanto sem ação.")
        return
    
    logging.info(f"Iniciando carga de dados para a tabela '{table_name}' no banco de dados.")

    engine = None

    try:
        engine = create_engine(connection_string)

        df.to_sql(table_name, engine, if_exists=if_exists, index=False)

        logging.info(f"Dados inseridos com sucesso na tabela {table_name}")

    except Exception as e:
        logging.error(f"Erro durante a carga de dados para o BD: {e}", exc_info=True)

    finally:
        if engine:
            engine.dispose()
            logging.info("Conexão com o banco de dados descartada")



