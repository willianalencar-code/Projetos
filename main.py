import streamlit as st
import pandas as pd
from datasets import load_dataset
import os

# ================================
# CONFIGURAÇÃO DA PÁGINA
# ================================
st.set_page_config(
    page_title="Sistema de Filtro e Exportação de Membros",
    layout="wide"
)

# ================================
# TOKEN HUGGING FACE (DIRETO NO CÓDIGO)
# ================================
HF_TOKEN = "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"

# ================================
# CARGA DE DADOS (HUGGING FACE)
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
    df["data_ultima_visita"] = pd.to_datetime(df["data_ultima_visita"], errors="coerce")
    df["data_ultima_compra"] = pd.to_datetime(df["data_ultima_compra"], errors="coerce")

    # Criação de STATUS DE COMPRA (REGRA DE NEGÓCIO)
    df["status_compra"] = df["data_ultima_compra"].isna().map(
        {True: "Nunca comprou", False: "Já comprou"}
    )

    return df

try:
    df = carregar_dados()

    # ================================
    # HEADER
    # ================================
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

    # Categoria
    categorias = st.sidebar.multiselect(
        "Categoria",
        options=sorted(df["categoria"].dropna().unique()),
        default=sorted(df["categoria"].dropna().unique())
    )

    # Setor
    setores = st.sidebar.multiselect(
        "Setor",
        options=sorted(df["setor"].dropna().unique()),
        default=sorted(df["setor"].dropna().unique())
    )

    # Status de Compra
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
    min_visita = df["data_ultima_visita"].min()
    max_visita = df["data_ultima_visita"].max()

    data_visita = st.sidebar.date_input(
        "Período da Última Visita",
        value=(min_visita, max_visita),
        min_value=min_visita,
        max_value=max_visita
    )

    # Última compra (apenas quem já comprou)
    df_com_compra = df[df["data_ultima_compra"].notna()]

    if not df_com_compra.empty:
        min_compra = df_com_compra["data_ultima_compra"].min()
        max_compra = df_com_compra["data_ultima_compra"].max()

        data_compra = st.sidebar.date_input(
            "Período da Última Compra (somente quem já comprou)",
            value=(min_compra, max_compra),
            min_value=min_compra,
            max_value=max_compra
        )
    else:
        data_compra = None

    # =================
