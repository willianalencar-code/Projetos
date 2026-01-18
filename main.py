import streamlit as st
import duckdb
from huggingface_hub import hf_hub_download
import pandas as pd

# ==========================================
# 1. CONFIGURAÇÕES DA PÁGINA
# ==========================================
st.set_page_config(page_title="Sistema de Clientes 7M", layout="wide", page_icon="📊")

@st.cache_resource
def get_dataset():
    """Baixa o arquivo do HF para o cache local"""
    try:
        # Puxa o token do segredo configurado no Streamlit Cloud
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

@st.cache_resource
def get_connection(path):
    """Cria a conexão DuckDB e a VIEW com as suas colunas"""
    con = duckdb.connect(database=':memory:')
    con.execute(f"CREATE VIEW clientes AS SELECT * FROM read_parquet('{path}')")
    return con

# ==========================================
# 2. INICIALIZAÇÃO
# ==========================================
caminho_arquivo = get_dataset()

if caminho_arquivo:
    con = get_connection(caminho_arquivo)
    
    st.title("📊 Gestão de Clientes - 7 Milhões")
    
    # ==========================================
    # 3. SIDEBAR - FILTROS REAIS
    # ==========================================
    st.sidebar.header("🔍 Filtros")
    
    # Filtro por member_pk (Busca exata ou parcial)
    id_busca = st.sidebar.text_input("Buscar por member_pk:")
    
    # Filtro dinâmico por Categoria
    categorias_df = con.execute("SELECT DISTINCT categoria FROM clientes WHERE categoria IS NOT NULL").df()
    cat_sel = st.sidebar.multiselect("Categorias:", categorias_df['categoria'].unique())
    
    # Filtro dinâmico por Setor
    setores_df = con.execute("SELECT DISTINCT setor FROM clientes WHERE setor IS NOT NULL").df()
    setor_sel = st.sidebar.multiselect("Setores:", setores_df['setor'].unique())

    # ==========================================
    # 4. CONSTRUÇÃO DA QUERY SQL
    # ==========================================
    query = "SELECT * FROM clientes WHERE 1=1"
    
    if id_busca:
        query += f" AND CAST(member_pk AS VARCHAR) LIKE '%{id_busca}%'"
    
    if cat_sel:
        query += f" AND categoria IN {tuple(cat_sel) if len(cat_sel) > 1 else f'({repr(cat_sel[0])})'}"
        
    if setor_sel:
        query += f" AND setor IN {tuple(setor_sel) if len(setor_sel) > 1 else f'({repr(setor_sel[0])})'}"

    # ==========================================
    # 5. PROCESSAMENTO E MÉTRICAS
    # ==========================================
    with st.spinner('Processando milhões de linhas...'):
        # Contagem total rápida
        total = con.execute(f"SELECT count(*) FROM ({query})").fetchone()[0]
        
        # Amostra de dados
        df_result = con.execute(query + " LIMIT 1000").df()
        
        # Converter colunas de data para formato legível no Pandas
        for col in ['data_ultima_visitadata_ultima_compra']: # Se estiverem juntas como string, ou separadas
             if col in df_result.columns:
                 df_result[col] = pd.to_datetime(df_result[col], errors='coerce')

    # Exibição de Métricas
    c1, c2 = st.columns(2)
    c1.metric("Clientes Encontrados", f"{total:,}")
    c2.metric("Base de Dados", "Hugging Face (Private)")

    # ==========================================
    # 6. GRÁFICOS ANALÍTICOS
    # ==========================================
    col_a, col_b = st.columns(2)
    
    with col_a:
        st.subheader("Volume por Setor")
        df_graf_setor = con.execute(f"SELECT setor, count(*) as total FROM ({query}) GROUP BY setor ORDER BY total DESC LIMIT 10").df()
        st.bar_chart(df_graf_setor.set_index('setor'))
        
    with col_b:
        st.subheader("Volume por Categoria")
        df_graf_cat = con.execute(f"SELECT categoria, count(*) as total FROM ({query}) GROUP BY categoria").df()
        st.pie_chart(df_graf_cat, values='total', names='categoria')

    # ==========================================
    # 7. TABELA FINAL
    # ==========================================
    st.subheader("📋 Detalhes (Amostra 1.000)")
    st.dataframe(df_result, use_container_width=True)

    # Exportação
    csv = df_result.to_csv(index=False).encode('utf-8')
    st.download_button("📥 Exportar Amostra CSV", csv, "segmentacao.csv", "text/csv")

else:
    st.warning("Aguardando configuração do Token nos Secrets do Streamlit Cloud.")
