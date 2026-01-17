import streamlit as st
import pandas as pd
import datetime

# 1. Configuração inicial da página
st.set_page_config(page_title="Dashboard de Membros", layout="wide")

# 2. Carregamento dos dados com cache para evitar custos de reprocessamento
@st.cache_data
def carregar_dados():
    # Carrega o dataset da raiz do repositório
    df = pd.read_csv('dataset.csv')
    
    # Converter colunas para datetime
    df['data_ultima_visita'] = pd.to_datetime(df['data_ultima_visita'])
    df['data_ultima_compra'] = pd.to_datetime(df['data_ultima_compra'])
    
    # PREENCHIMENTO DE NULOS (Tratamento para evitar erro NaTType)
    # Usamos 1900-01-01 para representar quem não tem data (ex: nunca comprou)
    data_padrao = pd.Timestamp('1900-01-01')
    df['data_ultima_visita'] = df['data_ultima_visita'].fillna(data_padrao)
    df['data_ultima_compra'] = df['data_ultima_compra'].fillna(data_padrao)
    
    # Converte para data simples (formato Python date) exigido pelo st.date_input
    df['data_ultima_visita'] = df['data_ultima_visita'].dt.date
    df['data_ultima_compra'] = df['data_ultima_compra'].dt.date
    
    return df

# Tenta carregar e processar a interface
try:
    df = carregar_dados()

    # --- INTERFACE ---
    st.title("📂 Sistema de Filtro e Exportação")

    # 3. Criação dos Filtros na Barra Lateral (Sidebar)
    st.sidebar.header("Filtros de Segmentação")

    # Filtro 1: Categoria
    categorias_selecionadas = st.sidebar.multiselect(
        "Selecione a Categoria:",
        options=sorted(df['categoria'].unique()),
        default=df['categoria'].unique()
    )

    # Filtro 2: Setor
    setores_selecionados = st.sidebar.multiselect(
        "Selecione o Setor:",
        options=sorted(df['setor'].unique()),
        default=df['setor'].unique()
    )

    # Definindo limites para os calendários baseados nos dados
    min_visita = df['data_ultima_visita'].min()
    max_visita = df['data_ultima_visita'].max()

    # Filtro 3: Data de Última Visita (Range)
    data_visita_range = st.sidebar.date_input(
        "Período de Última Visita:",
        value=(min_visita, max_visita),
        min_value=min_visita,
        max_value=max_visita
    )

    # Definindo limites para o filtro de compra
    min_compra = df['data_ultima_compra'].min()
    max_compra = df['data_ultima_compra'].max()

    # Filtro 4: Data de Última Compra (Range)
    data_compra_range = st.sidebar.date_input(
        "Período de Última Compra:",
        value=(min_compra, max_compra),
        min_value=min_compra,
        max_value=max_compra
    )

    # 4. Aplicando os Filtros
    # Filtro de Categoria e Setor
    mask = (df['categoria'].isin(categorias_selecionadas)) & (df['setor'].isin(setores_selecionados))

    # Filtro de Datas (Proteção para quando o usuário seleciona apenas uma data no range)
    if isinstance(data_visita_range, tuple) and len(data_visita_range) == 2:
        mask &= (df['data_ultima_visita'] >= data_visita_range[0]) & (df['data_ultima_visita'] <= data_visita_range[1])
    
    if isinstance(data_compra_range, tuple) and len(data_compra_range) == 2:
        mask &= (df['data_ultima_compra'] >= data_compra_range[0]) & (df['data_ultima_compra'] <= data_compra_range[1])

    df_filtrado = df[mask]

    # 5. Exibição de Métricas
    qtd_unique_members = df_filtrado['member_pk'].nunique()
    
    col1, col2 = st.columns(2)
    # Formatação brasileira para números (ponto para milhar)
    col1.metric("Membros Únicos (member_pk)", f"{qtd_unique_members:,}".replace(",", "."))
    col2.metric("Total de Registros Exibidos", f"{len(df_filtrado):,}".replace(",", "."))

    # 6. Área Central: Exibição dos Resultados
    st.subheader("📊 Visualização dos Dados")
    st.dataframe(df_filtrado, use_container_width=True)

    # 7. Botão de Exportação
    st.divider()
    csv_data = df_filtrado.to_csv(index=False).encode('utf-8')

    st.download_button(
        label="📥 Exportar Base Filtrada para CSV",
        data=csv_data,
        file_name='base_filtrada.csv',
        mime='text/csv',
    )

except FileNotFoundError:
    st.error("Erro: O arquivo 'dataset.csv' não foi encontrado no seu GitHub.")
except Exception as e:
    st.error(f"Ocorreu um erro inesperado: {e}")
