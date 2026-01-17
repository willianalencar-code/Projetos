import streamlit as st
import pandas as pd

# 1. Configuração inicial da página
st.set_page_config(page_title="Dashboard de Membros", layout="wide")

# 2. Carregamento dos dados
@st.cache_data
import datetime

@st.cache_data
def carregar_dados():
    df = pd.read_csv('dataset.csv')
    
    # Converter para datetime
    df['data_ultima_visita'] = pd.to_datetime(df['data_ultima_visita'])
    df['data_ultima_compra'] = pd.to_datetime(df['data_ultima_compra'])
    
    # PREENCHIMENTO DE NULOS:
    # Se não tem visita, usamos uma data muito antiga
    df['data_ultima_visita'] = df['data_ultima_visita'].fillna(pd.Timestamp('1900-01-01'))
    # Se não tem compra, usamos 1900-01-01 (indica que nunca comprou)
    df['data_ultima_compra'] = df['data_ultima_compra'].fillna(pd.Timestamp('1900-01-01'))
    
    # Converter para o formato de data simples do Python (necessário para o calendário)
    df['data_ultima_visita'] = df['data_ultima_visita'].dt.date
    df['data_ultima_compra'] = df['data_ultima_compra'].dt.date
    
    return df

# --- Dentro da barra lateral ---
# Agora garantimos que o calendário pegue a data mínima (que será 1900 se houver nulos)
min_visita = df['data_ultima_visita'].min()
max_visita = df['data_ultima_visita'].max()

data_visita_range = st.sidebar.date_input(
    "Período de Última Visita:",
    value=(min_visita, max_visita),
    min_value=min_visita,
    max_value=max_visita
)
try:
    df = carregar_dados()

    # --- INTERFACE ---
    st.title("📂 Sistema de Filtro e Exportação")

    # 3. Criação dos Filtros na Barra Lateral (Sidebar)
    st.sidebar.header("Filtros de Segmentação")

    # Filtro 1: Categoria
    categorias_selecionadas = st.sidebar.multiselect(
        "Selecione a Categoria:",
        options=df['categoria'].unique(),
        default=df['categoria'].unique()
    )

    # Filtro 2: Setor
    setores_selecionados = st.sidebar.multiselect(
        "Selecione o Setor:",
        options=df['setor'].unique(),
        default=df['setor'].unique()
    )

    # Filtro 3: Data de Última Visita (Range)
    min_visita = df['data_ultima_visita'].min().to_pydatetime()
    max_visita = df['data_ultima_visita'].max().to_pydatetime()
    data_visita_range = st.sidebar.date_input(
        "Período de Última Visita:",
        value=(min_visita, max_visita),
        min_value=min_visita,
        max_value=max_visita
    )

    # Filtro 4: Data de Última Compra (Range)
    min_compra = df['data_ultima_compra'].min().to_pydatetime()
    max_compra = df['data_ultima_compra'].max().to_pydatetime()
    data_compra_range = st.sidebar.date_input(
        "Período de Última Compra:",
        value=(min_compra, max_compra),
        min_value=min_compra,
        max_value=max_compra
    )

    # 4. Aplicando os Filtros
    # Filtro de texto/categoria
    mask = (df['categoria'].isin(categorias_selecionadas)) & (df['setor'].isin(setores_selecionados))

    # Filtro de datas (garantindo que o usuário selecionou o range completo [início, fim])
    if len(data_visita_range) == 2:
        mask &= (df['data_ultima_visita'].dt.date >= data_visita_range[0]) & (df['data_ultima_visita'].dt.date <= data_visita_range[1])
    
    if len(data_compra_range) == 2:
        mask &= (df['data_ultima_compra'].dt.date >= data_compra_range[0]) & (df['data_ultima_compra'].dt.date <= data_compra_range[1])

    df_filtrado = df[mask]

    # 5. Exibição de Métricas (Quantidade Unique member_pk)
    qtd_unique_members = df_filtrado['member_pk'].nunique()
    
    col1, col2 = st.columns(2)
    col1.metric("Membros Únicos (member_pk)", f"{qtd_unique_members:,}".replace(",", "."))
    col2.metric("Total de Registros", f"{len(df_filtrado):,}".replace(",", "."))

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
    st.error("Arquivo 'dataset.csv' não encontrado. Certifique-se de que ele está na raiz do seu repositório no GitHub.")
except Exception as e:
    st.error(f"Ocorreu um erro: {e}")
