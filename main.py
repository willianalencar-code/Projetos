import streamlit as st
import pandas as pd
from datasets import load_dataset
from huggingface_hub import login, whoami

# ================================
# SOLUÇÃO PARA O BUG DO STREAMLIT
# ================================
import warnings
warnings.filterwarnings("ignore")

# Configuração para evitar o erro de removeChild
st.set_option('deprecation.showfileUploaderEncoding', False)
st.set_option('deprecation.showPyplotGlobalUse', False)

# ================================
# CONFIGURAÇÃO DA PÁGINA
# ================================
st.set_page_config(
    page_title="Sistema de Filtro e Exportação de Membros",
    layout="wide",
    initial_sidebar_state="expanded"  # Adicione esta linha
)

# Adicione um container principal
main_container = st.container()

with main_container:
    # ================================
    # TOKEN E AUTENTICAÇÃO SEGURA
    # ================================
    # Use try-except para capturar erros de secrets
    try:
        HF_TOKEN = st.secrets["HF_TOKEN"]
    except (KeyError, FileNotFoundError):
        HF_TOKEN = None
        st.warning("⚠️ Token do Hugging Face não configurado")    
# ================================
# TOKEN E AUTENTICAÇÃO SEGURA
# ================================
# IMPORTANTE: O token agora vem do secrets.toml
HF_TOKEN = st.secrets.get("HF_TOKEN") if "HF_TOKEN" in st.secrets else None

def realizar_login():
    """Garante que o login seja feito apenas uma vez."""
    if "autenticado" not in st.session_state:
        try:
            if HF_TOKEN:
                # Verifica autenticação
                whoami(token=HF_TOKEN)
                st.session_state.autenticado = True
                st.sidebar.success("✅ Autenticado")
            else:
                st.error("⚠️ Token não configurado!")
                st.info("""
                **Como configurar:**
                1. Crie um arquivo `.streamlit/secrets.toml`
                2. Adicione: `HF_TOKEN = "seu_token_aqui"`
                3. Obtenha o token em: https://huggingface.co/settings/tokens
                """)
                st.stop()
        except Exception:
            # Realiza login se necessário
            if HF_TOKEN:
                login(token=HF_TOKEN)
                st.session_state.autenticado = True
                st.sidebar.success("✅ Login realizado")

# ================================
# FUNÇÃO PARA CARREGAR DADOS
# ================================
@st.cache_data(show_spinner="Carregando base de dados do Hugging Face...")
def carregar_dados():
    realizar_login()
    
    if not HF_TOKEN:
        st.error("Token não disponível")
        return pd.DataFrame()
    
    # Carrega o dataset especificando o token para evitar erros de permissão
    ds = load_dataset(
        "WillianAlencar/SegmentacaoClientes",
        split="train",
        token=HF_TOKEN,
        use_auth_token=True
    )

    df = ds.to_pandas()

    # Conversão de colunas para datetime
    for col in ["data_ultima_visita", "data_ultima_compra"]:
        df[col] = pd.to_datetime(df[col], errors="coerce")

    # Identificação de status de compra
    df["status_compra"] = df["data_ultima_compra"].isna().map(
        {True: "Nunca comprou", False: "Já comprou"}
    )

    return df

# ================================
# INTERFACE E FILTROS
# ================================
try:
    df = carregar_dados()
    
    if df.empty:
        st.warning("Nenhum dado carregado. Verifique a autenticação.")
        st.stop()

    st.title("📂 Sistema Profissional de Filtro e Exportação")

    # Bloco de Métricas Superiores
    c1, c2, c3 = st.columns(3)
    total = len(df)
    compradores = len(df[df["status_compra"] == "Já comprou"])
    
    c1.metric("Total de Clientes", total)
    c2.metric("Já Compraram", compradores)
    c3.metric("Nunca Compraram", total - compradores)

    st.markdown("---")

    # --- SIDEBAR: FILTROS ---
    st.sidebar.header("🔎 Parâmetros de Filtro")

    # Filtros básicos
    categorias = st.sidebar.multiselect(
        "Selecione a Categoria",
        options=sorted(df["categoria"].dropna().unique()),
        default=sorted(df["categoria"].dropna().unique())
    )

    setores = st.sidebar.multiselect(
        "Selecione o Setor",
        options=sorted(df["setor"].dropna().unique()),
        default=sorted(df["setor"].dropna().unique())
    )

    status_sel = st.sidebar.multiselect(
        "Status de Compra",
        options=["Nunca comprou", "Já comprou"],
        default=["Nunca comprou", "Já comprou"]
    )

    # --- DATAS ---
    st.sidebar.subheader("📅 Filtros Temporais")
    
    # Data de Visita
    min_v, max_v = df["data_ultima_visita"].min().date(), df["data_ultima_visita"].max().date()
    data_visita = st.sidebar.date_input(
        "Período de Última Visita", 
        value=(min_v, max_v),
        min_value=min_v,
        max_value=max_v
    )

    # Data de Compra (apenas para quem já comprou)
    df_c = df[df["data_ultima_compra"].notna()]
    if not df_c.empty:
        min_c, max_c = df_c["data_ultima_compra"].min().date(), df_c["data_ultima_compra"].max().date()
        data_compra = st.sidebar.date_input(
            "Período de Última Compra (apenas compradores)",
            value=(min_c, max_c),
            min_value=min_c,
            max_value=max_c
        )
    else:
        data_compra = None

    # ================================
    # PROCESSAMENTO DOS FILTROS
    # ================================
    
    # Filtro Primário (Categorias, Setores, Status e Visita)
    if isinstance(data_visita, tuple) and len(data_visita) == 2:
        mask_base = (
            (df["categoria"].isin(categorias)) &
            (df["setor"].isin(setores)) &
            (df["status_compra"].isin(status_sel)) &
            (df["data_ultima_visita"].dt.date.between(*data_visita))
        )
    else:
        mask_base = (
            (df["categoria"].isin(categorias)) &
            (df["setor"].isin(setores)) &
            (df["status_compra"].isin(status_sel))
        )
    
    df_filtrado = df[mask_base].copy()

    # Filtro Secundário (Data de Compra aplicada apenas a quem já comprou)
    if data_compra and isinstance(data_compra, tuple) and len(data_compra) == 2:
        if "Já comprou" in status_sel and len(df_filtrado) > 0:
            mask_data_compra = (
                (df_filtrado["data_ultima_compra"].notna() & 
                 df_filtrado["data_ultima_compra"].dt.date.between(*data_compra)) |
                (df_filtrado["status_compra"] == "Nunca comprou")
            )
            df_filtrado = df_filtrado[mask_data_compra]

    # ================================
    # RESULTADOS E EXPORTAÇÃO
    # ================================
    st.subheader(f"📊 Membros Filtrados: {len(df_filtrado)}")
    
    # Tabela principal
    if not df_filtrado.empty:
        st.dataframe(
            df_filtrado.reset_index(drop=True), 
            use_container_width=True,
            height=400
        )

        # Botão para Download
        csv = df_filtrado.to_csv(index=False).encode('utf-8')
        st.download_button(
            label="📥 Exportar Base Filtrada (CSV)",
            data=csv,
            file_name='extracao_membros.csv',
            mime='text/csv'
        )
    else:
        st.warning("Nenhum dado encontrado para os filtros selecionados.")

except Exception as e:
    st.error(f"❌ Ocorreu um erro: {str(e)}")
    st.info("Aguarde alguns minutos se o erro for de limite de requisições (Rate Limit).")
