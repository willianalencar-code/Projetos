import streamlit as st
import duckdb
from huggingface_hub import hf_hub_download
import pandas as pd

# ==========================================
# 1. CONFIGURAÇÕES DA PÁGINA
# ==========================================
st.set_page_config(
    page_title="Sistema de Clientes 7M", 
    layout="wide",
    page_icon="📊"
)

@st.cache_resource
def get_dataset():
    if "HF_TOKEN" not in st.secrets:
        st.error("Token não encontrado nos Secrets!")
        return None
        
    try:
        token_hf = "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"
        
        # O hf_hub_download precisa do token para repositórios privados
        caminho_local = hf_hub_download(
            repo_id="WillianAlencar/SegmentacaoClientes",
            filename="data/train-00000-of-00001.parquet",
            repo_type="dataset",
            token=token_hf  # <--- Garanta que esta linha está aqui
        )
        return caminho_local
    except Exception as e:
        # Se der erro 401 aqui, o token nos Secrets está inválido
        st.error(f"Erro de Autenticação: {e}")
        return None

@st.cache_resource
def get_connection(path):
    """Cria a conexão com DuckDB e uma VIEW para facilitar o SQL"""
    con = duckdb.connect(database=':memory:')
    # Criamos uma VIEW chamada 'clientes' para não ter que repetir o caminho do arquivo
    con.execute(f"CREATE VIEW clientes AS SELECT * FROM read_parquet('{path}')")
    return con

# ==========================================
# 2. INICIALIZAÇÃO DOS DADOS
# ==========================================
caminho_arquivo = get_dataset()

if caminho_arquivo:
    con = get_connection(caminho_arquivo)
    
    st.title("📊 Painel Analítico de Clientes")
    st.caption("Processando 7 milhões de registros em tempo real com DuckDB + Parquet")

    # ==========================================
    # 3. INTERFACE LATERAL (FILTROS)
    # ==========================================
    st.sidebar.header("🔍 Filtros de Pesquisa")
    
    nome_busca = st.sidebar.text_input("Buscar por Nome:")
    
    # Carregar categorias únicas para o filtro usando SQL (muito rápido)
    categorias_df = con.execute("SELECT DISTINCT categoria FROM clientes WHERE categoria IS NOT NULL").df()
    cat_selecionada = st.sidebar.multiselect("Categorias:", categorias_df['categoria'].tolist())

    # ==========================================
    # 4. CONSTRUÇÃO DA CONSULTA SQL
    # ==========================================
    # Base da query
    base_query = "SELECT * FROM clientes WHERE 1=1"
    
    if nome_busca:
        # ILIKE para busca case-insensitive
        base_query += f" AND nome ILIKE '%{nome_busca}%'"
    
    if cat_selecionada:
        if len(cat_selecionada) == 1:
            base_query += f" AND categoria = '{cat_selecionada[0]}'"
        else:
            base_query += f" AND categoria IN {tuple(cat_selecionada)}"

    # ==========================================
    # 5. EXECUÇÃO E MÉTRICAS
    # ==========================================
    with st.spinner('Consultando base de dados...'):
        # Total de registros filtrados
        total_filtrado = con.execute(f"SELECT count(*) FROM ({base_query})").fetchone()[0]
        
        # Soma total de gastos (agregação pesada feita no DuckDB)
        # Ajuste o nome da coluna 'valor_gasto' se for diferente no seu arquivo
        soma_gastos = con.execute(f"SELECT SUM(valor_gasto) FROM ({base_query})").fetchone()[0] or 0
        
        # Amostra para exibir na tabela
        df_exibicao = con.execute(base_query + " LIMIT 1000").df()

    # Exibição de Métricas
    m1, m2, m3 = st.columns(3)
    m1.metric("Clientes Encontrados", f"{total_filtrado:,}")
    m2.metric("Volume Financeiro", f"R$ {soma_gastos:,.2f}")
    m3.metric("Motor de Dados", "DuckDB ⚡")

    st.divider()

    # ==========================================
    # 6. GRÁFICOS E ANÁLISES
    # ==========================================
    col_left, col_right = st.columns(2)

    with col_left:
        st.subheader("Top 10 Setores")
        df_setor = con.execute(f"""
            SELECT setor, COUNT(*) as qtd 
            FROM ({base_query}) 
            GROUP BY setor 
            ORDER BY qtd DESC 
            LIMIT 10
        """).df()
        if not df_setor.empty:
            st.bar_chart(df_setor.set_index('setor'))
        else:
            st.info("Sem dados para exibir o gráfico.")

    with col_right:
        st.subheader("Maiores Compradores (Top 10)")
        df_top_compradores = con.execute(f"""
            SELECT nome, valor_gasto 
            FROM ({base_query}) 
            ORDER BY valor_gasto DESC 
            LIMIT 10
        """).df()
        st.dataframe(df_top_compradores, use_container_width=True)

    # ==========================================
    # 7. TABELA DE DADOS E DOWNLOAD
    # ==========================================
    st.subheader("📋 Detalhes dos Registros (Amostra de 1.000)")
    st.dataframe(df_exibicao, use_container_width=True, height=350)

    if not df_exibicao.empty:
        csv = df_exibicao.to_csv(index=False).encode('utf-8')
        st.download_button(
            label="📥 Baixar Amostra Filtrada (CSV)",
            data=csv,
            file_name="clientes_filtrados.csv",
            mime="text/csv"
        )
else:
    st.error("Não foi possível carregar os dados. Verifique seu Token e o nome do repositório.")
