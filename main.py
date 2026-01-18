import streamlit as st
import pandas as pd
import requests
import json
import io

# ================================
# CONFIGURAÇÃO
# ================================
st.set_page_config(
    page_title="Sistema de Clientes",
    layout="wide"
)

st.title("📊 Sistema de Clientes")

# ================================
# VERIFICAÇÃO DO TOKEN
# ================================
st.sidebar.header("Configuração")

# Verificar token
try:
    HF_TOKEN = st.secrets["HF_TOKEN"]
    token_preview = HF_TOKEN[:8] + "..." + HF_TOKEN[-4:] if len(HF_TOKEN) > 12 else "***"
    st.sidebar.success(f"✅ Token: {token_preview}")
except:
    HF_TOKEN = None
    st.sidebar.error("❌ Token não configurado")
    st.sidebar.info("Configure em `.streamlit/secrets.toml`")

# ================================
# MÉTODO ALTERNATIVO - VIA API
# ================================
def carregar_dados_api():
    """Carrega dados diretamente da API do Hugging Face"""
    if not HF_TOKEN:
        return None
    
    try:
        # URL do dataset no Hugging Face
        dataset_url = "https://datasets-server.huggingface.co/rows"
        params = {
            "dataset": "WillianAlencar/SegmentacaoClientes",
            "config": "default",
            "split": "train",
            "offset": 0,
            "length": 100  # Limite inicial
        }
        
        headers = {
            "Authorization": f"Bearer {HF_TOKEN}"
        }
        
        st.info("Conectando ao Hugging Face...")
        
        response = requests.get(dataset_url, params=params, headers=headers, timeout=30)
        
        if response.status_code == 200:
            data = response.json()
            
            if "rows" in data:
                # Converter para DataFrame
                rows = []
                for row in data["rows"]:
                    rows.append(row["row"])
                
                df = pd.DataFrame(rows)
                st.success(f"✅ {len(df)} registros carregados")
                return df
            else:
                st.error("Formato de resposta inesperado")
                return None
        else:
            st.error(f"Erro HTTP {response.status_code}")
            return None
            
    except Exception as e:
        st.error(f"Erro na API: {str(e)}")
        return None

# ================================
# MÉTODO SIMPLES - TESTE
# ================================
def carregar_dados_simples():
    """Carrega dados de exemplo se a API falhar"""
    st.warning("Usando dados de exemplo...")
    
    # Dados de exemplo
    data = {
        "nome": ["Cliente A", "Cliente B", "Cliente C", "Cliente D"],
        "categoria": ["VIP", "Regular", "VIP", "Regular"],
        "setor": ["Tecnologia", "Comércio", "Serviços", "Tecnologia"],
        "data_ultima_visita": ["2024-01-15", "2024-01-10", "2024-01-05", "2024-01-20"],
        "data_ultima_compra": ["2024-01-10", None, "2024-01-03", None],
        "valor_gasto": [1500, 500, 2000, 0]
    }
    
    df = pd.DataFrame(data)
    
    # Converter datas
    for col in ["data_ultima_visita", "data_ultima_compra"]:
        df[col] = pd.to_datetime(df[col], errors='coerce')
    
    # Status de compra
    df["status_compra"] = df["data_ultima_compra"].apply(
        lambda x: "Já comprou" if pd.notna(x) else "Nunca comprou"
    )
    
    return df

# ================================
# INTERFACE PRINCIPAL
# ================================
# Botões para diferentes métodos
st.sidebar.subheader("Opções de Carregamento")

opcao = st.sidebar.radio(
    "Escolha o método:",
    ["API Hugging Face", "Dados de Exemplo"]
)

# Carregar dados
if opcao == "API Hugging Face" and HF_TOKEN:
    with st.spinner("Carregando via API..."):
        df = carregar_dados_api()
        
        if df is None or df.empty:
            st.error("Falha ao carregar via API. Usando dados de exemplo...")
            df = carregar_dados_simples()
else:
    df = carregar_dados_simples()

# Se ainda não temos dados
if df is None or df.empty:
    st.error("Não foi possível carregar dados.")
    
    # Mostrar ajuda
    with st.expander("🔧 Solução de Problemas", expanded=True):
        st.markdown("""
        ### Problemas comuns:
        
        1. **Token inválido ou expirado**
           - Gere novo token em: https://huggingface.co/settings/tokens
           - Permissão: "Read"
        
        2. **Dataset não acessível**
           - Verifique se o dataset existe: https://huggingface.co/datasets/WillianAlencar/SegmentacaoClientes
        
        3. **Problemas de conexão**
           - Verifique firewall/proxy
        
        ### Configuração rápida:
        ```toml
        # .streamlit/secrets.toml
        HF_TOKEN = "hf_seu_token_aqui"
        ```
        """)
    
    st.stop()

# ================================
# EXIBIR DADOS
# ================================
st.success(f"✅ Dados carregados: {len(df)} registros")

# Visualizar
with st.expander("📋 Visualizar dados", expanded=True):
    st.dataframe(df, use_container_width=True)

# Estatísticas
st.subheader("📈 Estatísticas")
col1, col2, col3 = st.columns(3)

with col1:
    st.metric("Total", len(df))

with col2:
    if "status_compra" in df.columns:
        compradores = len(df[df["status_compra"] == "Já comprou"])
        st.metric("Já Compraram", compradores)

with col3:
    if "status_compra" in df.columns:
        nunca = len(df[df["status_compra"] == "Nunca comprou"])
        st.metric("Nunca Compraram", nunca)

# ================================
# FILTROS SIMPLES
# ================================
st.sidebar.header("🔍 Filtros")

if "categoria" in df.columns:
    categorias = st.sidebar.multiselect(
        "Categoria",
        options=sorted(df["categoria"].unique()),
        default=sorted(df["categoria"].unique())
    )
else:
    categorias = []

if "setor" in df.columns:
    setores = st.sidebar.multiselect(
        "Setor",
        options=sorted(df["setor"].unique()),
        default=sorted(df["setor"].unique())
    )
else:
    setores = []

# Aplicar filtros
df_filtrado = df.copy()

if categorias:
    df_filtrado = df_filtrado[df_filtrado["categoria"].isin(categorias)]

if setores:
    df_filtrado = df_filtrado[df_filtrado["setor"].isin(setores)]

# ================================
# RESULTADOS
# ================================
st.subheader(f"📊 Resultados Filtrados: {len(df_filtrado)}")

if not df_filtrado.empty:
    st.dataframe(df_filtrado, use_container_width=True, height=300)
    
    # Download
    csv = df_filtrado.to_csv(index=False).encode('utf-8')
    st.download_button(
        "📥 Exportar CSV",
        data=csv,
        file_name="clientes.csv",
        mime="text/csv"
    )
else:
    st.warning("Nenhum resultado com os filtros selecionados")
