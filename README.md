# Projeto de Engenharia e Análise de Dados: Otimização de Apps Google Play Store

## 🎯 Visão Geral do Projeto

Este projeto demonstra a construção e orquestração de um **pipeline de ingestão e processamento de dados (ETL)** robusto em Python. O objetivo foi transformar dados brutos de aplicativos da Google Play Store e avaliações de usuários em **ativos de dados estruturados e limpos**, prontos para análise e consumo por camadas de BI. O foco analítico foi em:

- Identificação de categorias de apps com potencial de crescimento.
    
- Avaliação da percepção dos usuários.
    
- Determinação dos fatores que influenciam a aceitação dos aplicativos.
    

O resultado é um **conjunto de dados modelado e persistido em SQLite**, servindo como uma camada analítica otimizada para ferramentas de Business Intelligence (BI), como o Power BI.

## ✨ Destaques da Engenharia de Dados

- **Arquitetura de Pipeline ETL Modular:** Implementação de um pipeline automatizado e escalável em Python, com módulos bem definidos para `extract`, `transform` e `load`, garantindo **separação de responsabilidades** e **manutenibilidade do código**.
    
- **Engenharia de Features e Qualidade de Dados:**
    
    - Desenvolvimento de lógicas de **limpeza de dados** (tratamento de valores ausentes, padronização de formatos e tipos).
        
    - Aplicação de **transformações complexas** para resolver problemas de granularidade, incluindo a **agregação de reviews por aplicativo** para criar métricas de sentimento concisas e evitar duplicações.
        
    - Preparação de dados para **análises temporais e de tendências**.
        
- **Persistência e Camadas de Dados:** Carregamento dos dados processados em um **banco de dados SQLite**, servindo como uma **camada Silver** (dados limpos e prontos para consumo), otimizada para modelagem e query em ferramentas de BI.
    
- **Automação e Orquestração:** Utilização de um script `main.py` para orquestrar o fluxo de dados de ponta a ponta, demonstrando **automação do processo de dados**.
    
- **Tratamento de Exceções:** Implementação robusta de `try-except` e `logging` para **monitoramento e resiliência do pipeline**.
    

## 📊 Resultados e Insights (Dashboard Power BI)

O dashboard interativo oferece uma visão clara sobre o desempenho dos aplicativos e a percepção dos usuários, validando a qualidade dos dados processados.

- **Potencial de Crescimento:** Identificação das categorias com maior volume de instalações e melhores ratings médios (ex: `GAME`, `COMMUNICATION`, `EDUCATION`), indicando **oportunidades de mercado**.
    
- **Percepção do Usuário:** Análise detalhada da distribuição de sentimentos (`Positive`, `Negative`, `Neutral`) e polaridade média por categoria, revelando a **satisfação do cliente**.
    
- **Fatores de Aceitação:** Demonstração da forte correlação entre preço e número de instalações (apps gratuitos dominam o mercado), e a importância do rating para o engajamento, fornecendo **direcionadores de produto**.
    

**🔗** [**Link para o Dashboard [Power BI]**](https://app.powerbi.com/view?r=eyJrIjoiY2E4Zjk3NzktMWY2MS00OThlLWJhYTYtNzJhMDIyZjRkZjViIiwidCI6ImMyYmQ1MWFmLWY4ZTctNDkwNC05MDg1LWRiNzVhNGU2ZGZlMiJ9)

## 🛠️ Tecnologias de Engenharia de Dados

- **Python 3.x** (Pandas para manipulação de DataFrames, NumPy para operações numéricas, SQLAlchemy para abstração de banco de dados)
    
- **SQLite:** Banco de dados relacional embutido para persistência da camada Silver.
    
- **Power BI Desktop:** Ferramenta de BI para visualização e modelagem de dados.
    
- **Git / GitHub:** Sistema de controle de versão para colaboração e gestão de código.
    
- **Logging:** Framework para monitoramento e depuração do pipeline.
    

## 🚀 Como Executar o Projeto

1. **Clone o repositório:** `git clone https://github.com/seu-usuario/seu-repositorio.git` `cd seu-repositorio`
    
2. **Configure o ambiente virtual e instale as dependências:** `python -m venv env` `.\env\Scripts\activate` (Windows) ou `source env/bin/activate` (Linux/macOS) `pip install pandas numpy sqlalchemy tabulate`
    
3. **Execute o pipeline ETL:** `python main.py` (Os dados limpos e transformados serão salvos em `output/googleplay_data_silver.sqlite`)
   
