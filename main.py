import streamlit as st
import duckdb
from huggingface_hub import hf_hub_download
import pandas as pd
import altair as alt

# ==========================================
# 1. CONFIGURAÇÃO DA PÁGINA
# ==========================================
st.set_page_config(
    page_title="Sistema de Clientes 7M",
    layout="wide",
    page_icon="📊"
)

# ==========================================
# 2. CACHE E CONEXÃO
# ==========================================
@st.cache_data(show_spinner=False)
def get_dataset():
    """Baixa o arquivo do HF para o cache local"""
    try:
        token = st.secrets["HF_TOKEN"]
        caminho_local = hf_hub_download(
            repo_id="WillianAlencar/SegmentacaoClientes",
            filename="data/train-00000-of-00001.parquet",
            repo_type="dataset",
            token=token
        )
        return caminho_local
    except Exception as e:
        st.error(f"Erro de conexão com o Hugging Face: {e}")
        return None

@st.cache_resource(show_spinner=False)
def get_connection():
    return duckdb.connect(database=':memory:')

# ==========================================
# 3. INICIALIZAÇÃO
# ==========================================
caminho_arquivo = get_dataset()
con = get_connection()

if caminho_arquivo:
    # Cria tabela temporária no DuckDB
    con.execute(f"CREATE OR REPLACE TABLE clientes AS SELECT * FROM read_parquet('{caminho_arquivo}')")
    
    st.title("📊 Gestão de Clientes - 7 Milhões")
    
    # ==========================================
    # 4. SIDEBAR - FILTROS
    # ==========================================
    st.sidebar.header("🔍 Filtros Dinâmicos")
    
    # --- Session state inicial ---
    if 'id_busca' not in st.session_state:
        st.session_state.id_busca = ""
    if 'cat_sel' not in st.session_state:
        st.session_state.cat_sel = []
    if 'setor_sel' not in st.session_state:
        st.session_state.setor_sel = []
    if 'date_visita_range' not in st.session_state:
        min_data_visita = con.execute("SELECT MIN(data_ultima_visita) FROM clientes").fetchone()[0]
        max_data_visita = con.execute("SELECT MAX(data_ultima_visita) FROM clientes").fetchone()[0]
        st.session_state.date_visita_range = [min_data_visita, max_data_visita]
    if 'date_compra_range' not in st.session_state:
        min_data_compra = con.execute("SELECT MIN(data_ultima_compra) FROM clientes").fetchone()[0]
        max_data_compra = con.execute("SELECT MAX(data_ultima_compra) FROM clientes").fetchone()[0]
        st.session_state.date_compra_range = [min_data_compra, max_data_compra]

    # --- Widgets usando session_state ---
    st.session_state.id_busca = st.sidebar.text_input(
        "Buscar por member_pk:",
        value=st.session_state.id_busca
    )

    categorias = con.execute("SELECT DISTINCT categoria FROM clientes WHERE categoria IS NOT NULL").df()['categoria'].tolist()
    st.session_state.cat_sel = st.sidebar.multiselect(
        "Categorias:",
        categorias,
        default=st.session_state.cat_sel
    )

    setores = con.execute("SELECT DISTINCT setor FROM clientes WHERE setor IS NOT NULL").df()['setor'].tolist()
    st.session_state.setor_sel = st.sidebar.multiselect(
        "Setores:",
        setores,
        default=st.session_state.setor_sel
    )

    st.session_state.date_visita_range = st.sidebar.date_input(
        "Período da última visita",
        value=st.session_state.date_visita_range
    )

    st.session_state.date_compra_range = st.sidebar.date_input(
        "Período da última compra",
        value=st.session_state.date_compra_range
    )

    # ==========================================
    # 5. BOTÃO PARA APLICAR FILTROS
    # ==========================================
    if st.sidebar.button("Aplicar Filtros"):
        # --- Monta query dinâmica ---
        query = "SELECT * FROM clientes WHERE 1=1"

        if st.session_state.id_busca:
            query += f" AND CAST(member_pk AS VARCHAR) LIKE '%{st.session_state.id_busca}%'"

        if st.session_state.cat_sel:
            placeholders = ', '.join([f"'{c}'" for c in st.session_state.cat_sel])
            query += f" AND categoria IN ({placeholders})"

        if st.session_state.setor_sel:
            placeholders = ', '.join([f"'{s}'" for s in st.session_state.setor_sel])
            query += f" AND setor IN ({placeholders})"

        if len(st.session_state.date_visita_range) == 2:
            start, end = st.session_state.date_visita_range
            query += f" AND data_ultima_visita BETWEEN '{start}' AND '{end}'"

        if len(st.session_state.date_compra_range) == 2:
            start, end = st.session_state.date_compra_range
            query += f" AND data_ultima_compra BETWEEN '{start}' AND '{end}'"

        # --- Processamento ---
        with st.spinner("Processando filtros..."):
            total = con.execute(f"SELECT COUNT(*) FROM ({query})").fetchone()[0]
            total_unicos = con.execute(f"SELECT COUNT(DISTINCT member_pk) FROM ({query})").fetchone()[0]

            df_result = con.execute(query + " LIMIT 1000").df()
            for col in ['data_ultima_visita', 'data_ultima_compra']:
                if col in df_result.columns:
                    df_result[col] = pd.to_datetime(df_result[col], errors='coerce')

        # --- Métricas ---
        c1, c2, c3 = st.columns(3)
        c1.metric("Total de Registros", f"{total:,}")
        c2.metric("Clientes Únicos", f"{total_unicos:,}")
        c3.metric("Base de Dados", "Hugging Face (Private)")

        # --- Gráficos ---
        col_a, col_b = st.columns(2)
        with col_a:
            st.subheader("Volume por Setor")
            df_graf_setor = con.execute(f"""
                SELECT setor, COUNT(*) AS total
                FROM ({query})
                GROUP BY setor
                ORDER BY total DESC
                LIMIT 10
            """).df()
            if not df_graf_setor.empty:
                chart = alt.Chart(df_graf_setor).mark_bar().encode(
                    x=alt.X('setor', sort='-y'),
                    y='total',
                    tooltip=['setor', 'total']
                )
                st.altair_chart(chart, use_container_width=True)

        with col_b:
            st.subheader("Volume por Categoria")
            df_graf_cat = con.execute(f"""
                SELECT categoria, COUNT(*) AS total
                FROM ({query})
                GROUP BY categoria
                ORDER BY total DESC
            """).df()
            if not df_graf_cat.empty:
                chart = alt.Chart(df_graf_cat).mark_bar().encode(
                    x=alt.X('categoria', sort='-y'),
                    y='total',
                    tooltip=['categoria', 'total']
                )
                st.altair_chart(chart, use_container_width=True)

        # --- Tabela ---
        st.subheader("📋 Detalhes da Amostra (1.000 linhas)")
        st.dataframe(df_result, use_container_width=True)

        # --- Exportação CSV ---
        csv = df_result.to_csv(index=False).encode('utf-8')
        st.download_button("📥 Exportar Amostra CSV", csv, "segmentacao.csv", "text/csv")

else:
    st.warning("Aguardando configuração do Token nos Secrets do Streamlit Cloud.")
