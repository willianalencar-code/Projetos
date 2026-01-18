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
    """Cria a conexão DuckDB em memória"""
    con = duckdb.connect(database=':memory:')
    return con

# ==========================================
# 3. INICIALIZAÇÃO
# ==========================================
caminho_arquivo = get_dataset()
con = get_connection()

if caminho_arquivo:
    # Criar ou recriar tabela temporária
    con.execute(f"CREATE OR REPLACE TABLE clientes AS SELECT * FROM read_parquet('{caminho_arquivo}')")
    
    st.title("📊 Gestão de Clientes - 7 Milhões")
    
    # ==========================================
    # 4. SIDEBAR - FILTROS DINÂMICOS
    # ==========================================
    st.sidebar.header("🔍 Filtros Dinâmicos")
    
    # Filtro por member_pk (partial match)
    id_busca = st.sidebar.text_input("Buscar por member_pk:")
    
    # Filtros por Categoria e Setor
    categorias = con.execute("SELECT DISTINCT categoria FROM clientes WHERE categoria IS NOT NULL").df()['categoria'].tolist()
    cat_sel = st.sidebar.multiselect("Categorias:", categorias)
    
    setores = con.execute("SELECT DISTINCT setor FROM clientes WHERE setor IS NOT NULL").df()['setor'].tolist()
    setor_sel = st.sidebar.multiselect("Setores:", setores)
    
    # ==========================================
    # 5. QUERY DINÂMICA COM FILTROS
    # ==========================================
    query = "SELECT * FROM clientes WHERE 1=1"
    
    if id_busca:
        query += f" AND CAST(member_pk AS VARCHAR) LIKE '%{id_busca}%'"
    
    if cat_sel:
        cat_tuple = tuple(cat_sel)
        if len(cat_tuple) == 1:
            query += f" AND categoria = '{cat_tuple[0]}'"
        else:
            query += f" AND categoria IN {cat_tuple}"
    
    if setor_sel:
        setor_tuple = tuple(setor_sel)
        if len(setor_tuple) == 1:
            query += f" AND setor = '{setor_tuple[0]}'"
        else:
            query += f" AND setor IN {setor_tuple}"
    
    # ==========================================
    # 6. PROCESSAMENTO E MÉTRICAS
    # ==========================================
    with st.spinner('Processando dados...'):
        # Contagem total de registros filtrados
        total = con.execute(f"SELECT COUNT(*) FROM ({query})").fetchone()[0]
        total_unicos = con.execute(f"SELECT COUNT(DISTINCT member_pk) FROM ({query})").fetchone()[0]
        
        # Amostra limitada para exibição
        df_result = con.execute(query + " LIMIT 1000").df()
        
        # Converter colunas de data
        for col in ['data_ultima_visita', 'data_ultima_compra']:
            if col in df_result.columns:
                df_result[col] = pd.to_datetime(df_result[col], errors='coerce')
    
    # Exibição de métricas
    c1, c2, c3 = st.columns(3)
    c1.metric("Total de Registros", f"{total:,}")
    c2.metric("Clientes Únicos", f"{total_unicos:,}")
    c3.metric("Base de Dados", "Hugging Face (Private)")
    
    # ==========================================
    # 7. GRÁFICOS INTERATIVOS
    # ==========================================
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
    
    # ==========================================
    # 8. TABELA DETALHADA
    # ==========================================
    st.subheader("📋 Detalhes da Amostra (1.000 linhas)")
    st.dataframe(df_result, use_container_width=True)
    
    # Exportação CSV
    csv = df_result.to_csv(index=False).encode('utf-8')
    st.download_button("📥 Exportar Amostra CSV", csv, "segmentacao.csv", "text/csv")

else:
    st.warning("Aguardando configuração do Token nos Secrets do Streamlit Cloud.")
