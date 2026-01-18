import streamlit as st
import pandas as pd
from datasets import load_dataset
from huggingface_hub import login
import warnings

# ================================
# CONFIGURAÇÃO INICIAL
# ================================
warnings.filterwarnings("ignore")

# Configuração da página - versão simplificada
st.set_page_config(
    page_title="Sistema de Filtro e Exportação",
    layout="wide"
)

# ================================
# TOKEN E AUTENTICAÇÃO
# ================================
# VERIFICAÇÃO SIMPLES DO TOKEN
try:
    HF_TOKEN = st.secrets["HF_TOKEN"]
    st.sidebar.success("✅ Token configurado")
except:
    HF_TOKEN = None
    st.sidebar.warning("⚠️ Token não encontrado em secrets.toml")
    st.sidebar.info("""
    **Para configurar:**
    1. Crie `.streamlit/secrets.toml`
    2. Adicione: `HF_TOKEN = "seu_token"`
    3. Obtenha token em: huggingface.co/settings/tokens
    """)

# ================================
# FUNÇÃO PARA CARREGAR DADOS
# ================================
@st.cache_data(ttl=3600)  # Cache por 1 hora
def carregar_dados():
    """Carrega dados do Hugging Face."""
    if not HF_TOKEN:
        return pd.DataFrame()
    
    try:
        # Login simples
        login(token=HF_TOKEN, add_to_git_credential=False)
        
        # Carregar dataset
        ds = load_dataset(
            "WillianAlencar/SegmentacaoClientes",
            split="train",
            token=HF_TOKEN,
            trust_remote_code=True
        )
        
        # Converter para pandas
        df = pd.DataFrame(ds)
        
        # Converter datas se existirem as colunas
        date_columns = ["data_ultima_visita", "data_ultima_compra"]
        for col in date_columns:
            if col in df.columns:
                df[col] = pd.to_datetime(df[col], errors='coerce')
        
        # Adicionar status de compra
        if "data_ultima_compra" in df.columns:
            df["status_compra"] = df["data_ultima_compra"].apply(
                lambda x: "Já comprou" if pd.notna(x) else "Nunca comprou"
            )
        else:
            df["status_compra"] = "Nunca comprou"
        
        return df
        
    except Exception as e:
        st.error(f"❌ Erro ao carregar dados: {str(e)[:100]}...")
        return pd.DataFrame()

# ================================
# INTERFACE PRINCIPAL
# ================================
def main():
    st.title("📊 Sistema de Filtro e Exportação de Clientes")
    
    # Botão para recarregar
    if st.button("🔄 Carregar Dados"):
        st.cache_data.clear()
        st.rerun()
    
    # Carregar dados com spinner
    with st.spinner("Carregando dados do Hugging Face..."):
        df = carregar_dados()
    
    # Verificar se dados foram carregados
    if df.empty:
        st.error("Não foi possível carregar os dados. Verifique:")
        st.info("""
        1. Token do Hugging Face está correto
        2. Conexão com internet
        3. Permissões do dataset
        """)
        return
    
    # Exibir estatísticas básicas
    st.success(f"✅ Dados carregados: {len(df)} registros")
    
    # Mostrar prévia dos dados
    with st.expander("📋 Visualizar dados brutos", expanded=False):
        st.dataframe(df.head(10))
    
    # ================================
    # FILTROS SIMPLES
    # ================================
    st.sidebar.header("🔍 Filtros")
    
    # Filtro por categoria se existir
    if "categoria" in df.columns:
        categorias = st.sidebar.multiselect(
            "Categorias",
            options=sorted(df["categoria"].dropna().unique()),
            default=sorted(df["categoria"].dropna().unique())[:3] if len(df) > 0 else []
        )
    else:
        categorias = []
    
    # Filtro por setor se existir
    if "setor" in df.columns:
        setores = st.sidebar.multiselect(
            "Setores",
            options=sorted(df["setor"].dropna().unique()),
            default=sorted(df["setor"].dropna().unique())[:3] if len(df) > 0 else []
        )
    else:
        setores = []
    
    # Filtro por status
    if "status_compra" in df.columns:
        status_opcoes = ["Nunca comprou", "Já comprou"]
        status_selecionados = st.sidebar.multiselect(
            "Status de Compra",
            options=status_opcoes,
            default=status_opcoes
        )
    else:
        status_selecionados = []
    
    # ================================
    # APLICAR FILTROS
    # ================================
    df_filtrado = df.copy()
    
    # Aplicar filtros condicionalmente
    if categorias and "categoria" in df_filtrado.columns:
        df_filtrado = df_filtrado[df_filtrado["categoria"].isin(categorias)]
    
    if setores and "setor" in df_filtrado.columns:
        df_filtrado = df_filtrado[df_filtrado["setor"].isin(setores)]
    
    if status_selecionados and "status_compra" in df_filtrado.columns:
        df_filtrado = df_filtrado[df_filtrado["status_compra"].isin(status_selecionados)]
    
    # ================================
    # EXIBIR RESULTADOS
    # ================================
    st.header(f"📈 Resultados: {len(df_filtrado)} registros")
    
    # Métricas
    col1, col2, col3 = st.columns(3)
    with col1:
        st.metric("Total Filtrado", len(df_filtrado))
    with col2:
        if "status_compra" in df_filtrado.columns:
            compradores = len(df_filtrado[df_filtrado["status_compra"] == "Já comprou"])
            st.metric("Já Compraram", compradores)
    with col3:
        if "status_compra" in df_filtrado.columns:
            nunca_comprou = len(df_filtrado[df_filtrado["status_compra"] == "Nunca comprou"])
            st.metric("Nunca Compraram", nunca_comprou)
    
    # Tabela de resultados
    if not df_filtrado.empty:
        st.dataframe(
            df_filtrado,
            use_container_width=True,
            height=400
        )
        
        # Botão de download
        csv = df_filtrado.to_csv(index=False).encode('utf-8')
        st.download_button(
            label="📥 Baixar CSV",
            data=csv,
            file_name="clientes_filtrados.csv",
            mime="text/csv",
            key="download_csv"
        )
    else:
        st.warning("Nenhum resultado encontrado com os filtros atuais.")

# ================================
# EXECUTAR APLICATIVO
# ================================
if __name__ == "__main__":
    main()
