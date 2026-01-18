import streamlit as st
import pandas as pd
from datasets import load_dataset

# ================================
# CONFIGURAÇÃO DA PÁGINA
# ================================
st.set_page_config(
    page_title="Sistema de Filtro e Exportação de Membros",
    layout="wide"
)

# ================================
# TOKEN HUGGING FACE
# ================================
HF_TOKEN = "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"

# ================================
# FUNÇÃO PARA CARREGAR DADOS
# ================================
@st.cache_data(show_spinner="Carregando dataset...")
def carregar_dados():
    ds = load_dataset(
        "WillianAlencar/SegmentacaoClientes",
        split="train",
        token=HF_TOKEN
    )

    df = ds.to_pandas()

    # Conversão de datas
    for col in ["data_ultima_visita", "data_ultima_compra"]:
        df[col] = pd.to_datetime(df[col], errors="coerce")

    # Status de compra
    df["status_compra"] = df["data_ultima_compra"].isna().map(
        {True: "Nunca comprou", False: "Já comprou"}
    )

    return df

# ================================
# CARREGAMENTO E FILTROS
# ================================
try:
    df = carregar_dados()

    # TÍTULO E REGRAS
    st.title("📂 Sistema Profissional de Filtro e Exportação")
    st.markdown(
        """
        **Regras de Negócio**
        - *Nunca comprou*: cliente sem data de compra registrada  
        - Filtros de data de compra afetam **somente quem já comprou**  
        - Nenhuma data fictícia é utilizada  
        """
    )

    # ================================
    # SIDEBAR - FILTROS
    # ================================
    st.sidebar.header("🔎 Filtros")

    categorias = st.sidebar.multiselect(
        "Categoria",
        options=sorted(df["categoria"].dropna().unique()),
        default=sorted(df["categoria"].dropna().unique())
    )

    setores = st.sidebar.multiselect(
        "Setor",
        options=sorted(df["setor"].dropna().unique()),
        default=sorted(df["setor"].dropna().unique())
    )

    status_compra = st.sidebar.multiselect(
        "Status de Compra",
        options=["Nunca comprou", "Já comprou"],
        default=["Nunca comprou", "Já comprou"]
    )

    # ================================
    # FILTROS DE DATA
    # ================================
    st.sidebar.subheader("📅 Datas")

    # Última visita
    data_visita = st.sidebar.date_input(
        "Período da Última Visita",
        value=(df["data_ultima_visita"].min(), df["data_ultima_visita"].max()),
        min_value=df["data_ultima_visita"].min(),
        max_value=df["data_ultima_visita"].max()
    )

    # Última compra (apenas quem já comprou)
    df_com_compra = df[df["data_ultima_compra"].notna()]
    if not df_com_compra.empty:
        data_compra = st.sidebar.date_input(
            "Período da Última Compra (somente quem já comprou)",
            value=(df_com_compra["data_ultima_compra"].min(), df_com_compra["data_ultima_compra"].max()),
            min_value=df_com_compra["data_ultima_compra"].min(),
            max_value=df_com_compra["data_ultima_compra"].max()
        )
    else:
        data_compra = None

    # ================================
    # APLICAÇÃO DOS FILTROS
    # ================================
    df_filtrado = df[
        (df["categoria"].isin(categorias)) &
        (df["setor"].isin(setores)) &
        (df["status_compra"].isin(status_compra)) &
        (df["data_ultima_visita"].between(*data_visita))
    ]

    if data_compra:
        df_filtrado = df_filtrado[
            (df_filtrado["data_ultima_compra"].between(*data_compra)) |
            (df_filtrado["status_compra"] == "Nunca comprou")
        ]

    # ================================
    # TABELA FINAL
    # ================================
    st.subheader(f"📊 Membros Filtrados ({len(df_filtrado)})")
    st.dataframe(df_filtrado.reset_index(drop=True))

except Exception as e:
    st.error(f"❌ Erro ao carregar os dados: {e}")
