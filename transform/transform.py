import pandas as pd
import numpy as np
import logging


# Esta função trata especificamente da tabela de aplicativos.
def transform_google_play_apps(df_apps: pd.DataFrame) -> pd.DataFrame:
    logging.info("Iniciando transformação do DataFrame de Apps...")

    # Conceito: Imutabilidade.
        # Criar uma cópia do DataFrame original (df_apps_raw)
    df_apps_transformed = df_apps.copy()

    # Conceito: Tratamento de valores ausentes (Missing Values).
        # fillna preenche os valores nulos na coluna Rating
        # A decisão de usar a median (mediana) é uma escolha estatística sólida, pois ela é menos sensível a outliers do que a média
    df_apps_transformed['Rating'].fillna(df_apps_transformed['Rating'].median(), inplace=True)

    # Conceito: Remoção de dados.
        # Remove linhas inteiras onde as colunas listadas (Type, Content Rating, etc.) têm valores ausentes
        # Função dropna remove linhas inteiras onde as colunas listadas (Type, Content Rating, etc.) têm valores ausentes
    df_apps_transformed.dropna(subset=['Type', 'Content Rating', 'Current Ver', 'Android Ver'], inplace=True)


    # Conceito: Padronização e Limpeza.
        # .str.replace('+', '')  e pd.to_numeric(..., errors='coerce') Linhas de código como .str.replace('+', '') e pd.to_numeric(..., errors='coerce') são exemplos de limpeza de dados. Elas removem caracteres não numéricos e convertem a coluna para um tipo numérico. 
        # O parâmetro errors='coerce' é uma prática de segurança que transforma valores que não podem ser convertidos em números em NaN, que podem ser tratados posteriormente.
    df_apps_transformed = df_apps_transformed[df_apps_transformed['Installs'] != 'Free']

    df_apps_transformed['Installs'] = df_apps_transformed['Installs'].astype(str).str.replace('+', '', regex=False).str.replace(',', '', regex=False)
    df_apps_transformed['Installs'] = pd.to_numeric(df_apps_transformed['Installs'], errors='coerce').astype(float)
    df_apps_transformed['Installs'].fillna(0, inplace=True)

    df_apps_transformed['Price'] = df_apps_transformed['Price'].astype(str).str.replace('$', '', regex=False)
    df_apps_transformed['Price'] = pd.to_numeric(df_apps_transformed['Price'], errors='coerce')
    df_apps_transformed['Price'].fillna(0, inplace=True)


    # Conceito: Encapsulamento de lógica complexa
         #  Em vez de poluir o código principal com uma lógica de conversão, você criou uma pequena função aninhada, facilitando o entendimento. 
         #  Objetivo: Converter os valores da coluna Size para uma unidade numérica consistente (megabytes - MB) e tratar valores não numéricos.
         #  Convert_size é uma ferramenta de limpeza e padronização que garante que a sua coluna de tamanho dos aplicativos seja numérica e consistente
    def convert_size(size):
        if isinstance(size, str): # Verifica o Tipo de Dado: verifica se o valor na coluna Size é uma string (texto), necessária porque o script irá processar linha por linha, e alguns valores podem já estar em um formato numérico ou serem nulos
            size = size.replace(',', '') # Remove Vírgulas: remove as vírgulas do texto, o que é um passo de limpeza fundamental para converter strings numéricas formatadas (ex: "1,000") em números.
            if 'M' in size: # Lida com Unidades de Medida: Se a string contém 'M', a função remove o 'M' e converte o restante para um número decimal (megabytes).
                return float(size.replace('M', ''))
            elif 'k' in size: 
                return float(size.replace('k', '')) / 1024
            elif 'Varies with device' in size: # # Trata Valores Inconsistentes:  elif 'Varies with device' in size:Alguns aplicativos têm um tamanho que varia de acordo com o dispositivo. A função identifica este padrão e o converte para np.nan (Not a Number). Isso é crucial para que o valor seja tratado como um dado ausente e não cause um erro no cálculo da mediana, que é o próximo passo
                return np.nan
        return float(size) # Se o valor não se encaixa em nenhuma das condições acima (ou se o valor já era um número), a função o converte para o tipo float.


     # Conceito: Aplicação de função
        # .apply(convert_size): O método .apply() executa a função convert_size em cada valor da coluna Size do DataFrame. Ele passa cada valor para a função, que retorna o valor limpo e padronizado.
    df_apps_transformed['Size'] = df_apps_transformed['Size'].apply(convert_size)
    # Conceito: Tratamento de valores ausentes.
        # Após a conversão, a coluna pode ter valores np.nan (do passo 4). Esta linha preenche esses valores ausentes com a mediana de todos os tamanhos de aplicativos, uma abordagem robusta para evitar distorções na análise.
    df_apps_transformed['Size'].fillna(df_apps_transformed['Size'].median(), inplace=True)

        # O errors='coerce' é um parâmetro muito útil na função que lida com erros de conversão de forma flexível
        # Quando você tenta converter uma coluna para um tipo de dado específico (como data ou número), pode haver valores que não se encaixam nesse formato.
        # Sem errors='coerce' (padrão é errors='raise'): Se a função encontrar um valor que não consegue converter, ela irá gerar um erro (ValueError) e interromper a execução do seu script.
        # Com errors='coerce': Se a função encontrar um valor que não consegue converter, em vez de gerar um erro, ela irá substituir esse valor por NaT (Not a Time, para datas) ou NaN (Not a Number, para números).
        # Ao usar errors='coerce', você garante que o script não vai quebrar se encontrar uma data inválid
        # Os valores inválidos serão convertidos para NaT, permitindo que você os identifique e trate posteriormente (por exemplo, preenchendo-os com a mediana ou a moda, ou removendo as linhas, dependendo da sua estratégia).
        # torna o seu processo de transformação mais robusto e tolerante a falhas
    df_apps_transformed['Last Updated'] = pd.to_datetime(df_apps_transformed['Last Updated'], errors='coerce')


    # Conceito: limpar e padronizar dados textuais de forma eficiente

        # Esta linha simplifica a coluna Genres, garantindo que cada aplicativo tenha apenas um gênero principal, mesmo que a entrada original liste múltiplos gêneros. Por exemplo, se um app tinha o gênero "Art & Design;Pretend Play", ele será simplificado para "Art & Design".
        
        # O método .apply(): é usado para executar uma função (neste caso, uma função lambda) em cada valor da coluna Genres
        # Conceito: apply é superpoderoso para aplicar lógicas personalizadas a colunas inteiras de um DataFrame.

        # lambda x: ...: Define uma pequena função anônima (sem nome) que opera sobre cada valor (x) da coluna Genres.
        # Conceito: Funções lambda são concisas e ideais para operações simples e de uso único

        # if isinstance(x, str): Verifica se o valor atual (x) é uma string.
        # Conceito: isinstance() é crucial para lidar com dados que podem ter tipos mistos (por exemplo, algumas células podem ser strings e outras podem ser números ou NaN). Se x não for uma string (por exemplo, for NaN), a condição if é falsa e o valor x é retornado sem alteração, evitando erros.
        # x.split(';')[0]: Se x for uma string, este trecho a divide usando o caractere ; como separador e pega o primeiro elemento ([0]) do resultado.
        # Conceito: O método .split() é comum para quebrar strings em listas de substrings. [0] acessa o primeiro item da lista resultante, que é o gênero principal.


    df_apps_transformed['Genres'] = df_apps_transformed['Genres'].apply(lambda x: x.split(';')[0] if isinstance(x, str) else x)


    # Esta linha remove linhas do DataFrame onde a coluna Category contém valores inválidos ou desalinhados que não representam uma categoria de texto real. Um exemplo notório neste dataset é uma linha onde "1.9" aparece na coluna Category, indicando um erro de parsing
    # df_apps_transformed[...]: Isso é chamado de indexação booleana ou filtragem booleana. O que está dentro dos colchetes é uma série de valores True ou False. Apenas as linhas onde o valor é True são mantidas no DataFrame

# Conceito: É uma forma muito eficiente de selecionar subconjuntos de dados com base em condições
    # df_apps_transformed['Category'].apply(lambda x: ...): Novamente, aplica uma função lambda a cada valor (x) da coluna Category
    # isinstance(x, str): Primeiro, verifica se o valor (x) é uma string. Se não for (por exemplo, se for NaN), a condição é falsa e a linha será removida
    # and not x.replace('.', '', 1).isdigit(): Se x for uma string, esta é a segunda parte da condição:
            # x.replace('.', '', 1): Substitui o primeiro ponto (.) encontrado na string por nada. Isso é feito para lidar com números decimais como "1.9".
            # .isdigit(): Verifica se a string resultante (após a possível remoção do ponto) consiste apenas de dígitos. Se for True, significa que o valor é um número.
            # not ...: Inverte o resultado de .isdigit(). Ou seja, a condição not x.replace('.', '', 1).isdigit() será True se o valor não for um número (após o tratamento do ponto).
# Conceito: A combinação isinstance(x, str) and not x.replace('.', '', 1).isdigit() cria uma condição que é True apenas para strings que são categorias válidas (não são números e são strings). Isso efetivamente filtra e remove as linhas com categorias malformadas.

    df_apps_transformed = df_apps_transformed[df_apps_transformed['Category'].apply(lambda x: isinstance(x, str) and not x.replace('.', '', 1).isdigit())]
    
    logging.info("Transformação do DataFrame Apps concluída.")

    return df_apps_transformed


# Esta função se concentra na tabela de reviews.
def transform_user_reviews(df_reviews: pd.DataFrame) -> pd.DataFrame:
    logging.info("Iniciando a transformação do DataFrame de Reviews...")

    # Imutabilidade: Criar uma cópia do DataFrame original (df_reviews_raw)
    df_reviews_transformed = df_reviews.copy()

    # A decisão de usar dropna em vez de fillna é estratégica: não há como inferir o sentimento de um review ausente, então o melhor a fazer é remover a linha.
    df_reviews_transformed.dropna(subset=['Translated_Review', 'Sentiment', 'Sentiment_Polarity', 'Sentiment_Subjectivity'], inplace=True)
    df_reviews_transformed['Sentiment_Polarity'] = pd.to_numeric(df_reviews_transformed['Sentiment_Polarity'], errors='coerce')
    df_reviews_transformed['Sentiment_Subjectivity'] = pd.to_numeric(df_reviews_transformed['Sentiment_Subjectivity'], errors='coerce')
    df_reviews_transformed.dropna(subset=['Sentiment_Polarity', 'Sentiment_Subjectivity'], inplace=True)
    
    logging.info("Transformação do DataFrame de Reviews concluída.")

    return df_reviews_transformed


 # Conceito: Engenharia de Features
    # Esta é a função mais sofisticada do seu módulo.
def aggregate_reviews(df_reviews_transformed: pd.DataFrame) -> pd.DataFrame:
    logging.info("Agregando reviews por app...")

    # Criam novas colunas que são o resultado de uma transformação de dados. Isso prepara o DataFrame para a próxima etapa, que é a agregação.
    df_reviews_transformed['Positive_Count'] = df_reviews_transformed['Sentiment'].apply(lambda x: 1 if x == 'Positive' else 0)
    df_reviews_transformed['Negative_Count'] = df_reviews_transformed['Sentiment'].apply(lambda x: 1 if x == 'Negative' else 0)
    df_reviews_transformed['Neutral_Count'] = df_reviews_transformed['Sentiment'].apply(lambda x: 1 if x == 'Neutral' else 0)

 # Conceito: Resumo de dados - Isso resolve o problema de granularidade e evita a duplicação de dados.
    #  O groupby é uma das operações mais poderosas do pandas. Ele agrupa as reviews de cada app em um único "pacote"
    #  Em seguida, .agg() executa várias operações de resumo (média, soma, contagem) sobre esses pacotes, criando uma única linha para cada app
    aggregated_reviews = df_reviews_transformed.groupby('App').agg(
        Avg_Sentiment_Polarity=('Sentiment_Polarity', 'mean'),
        Avg_Sentiment_Subjectivity=('Sentiment_Subjectivity', 'mean'),
        Total_Reviews=('Translated_Review', 'count'),
        Positive_Reviews=('Positive_Count', 'sum'),
        Negative_Reviews=('Negative_Count', 'sum'),
        Neutral_Reviews=('Neutral_Count', 'sum')
    ).reset_index()

    logging.info("Reviews agregadas com sucesso.")
    return aggregated_reviews


 # Conceito: Junção (Merge)
    # Esta função combina as duas tabelas. O how='left' significa que ele manterá todos os aplicativos da tabela da esquerda (df_apps)
    # E e adicionará as informações de reviews agregadas da tabela da direita (df_reviews), Se um app não tiver reviews, as colunas de reviews ficarão com valores nulos
def unify_dataframes(df_apps: pd.DataFrame, df_reviews: pd.DataFrame) -> pd.DataFrame:
    logging.info("Iniciando a unificação dos DataFrames...")

    if 'App' not in df_apps.columns or 'App' not in df_reviews.columns:
        logging.error("Erro: A coluna 'App' não está presente em um ou ambos os DataFrames. Não é possível unificar.")
        return None
    
    df_apps['App'] = df_apps['App'].astype(str)
    df_reviews['App'] = df_reviews['App'].astype(str)
    df_unified = pd.merge(df_apps, df_reviews, on='App', how='left')

    logging.info("DataFrames unificados com sucesso.")
    logging.info(f"DataFrame unificado tem {len(df_unified)} linhas e {len(df_unified.columns)} colunas.")

    return df_unified
    
