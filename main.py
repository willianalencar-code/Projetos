import streamlit as st
import pandas as pd
from datasets import load_dataset
from huggingface_hub import login # Importação necessária para autenticação

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
# Recomendação: Use st.secrets["HF_TOKEN"] no Streamlit Cloud
HF_TOKEN = "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"

# ================================
# FUNÇÃO PARA CARREGAR DADOS
# ================================
@st.cache_data(show_spinner="Autenticando e carregando dataset...")
def carregar_dados():
    # Realiza o login no Hugging Face antes de tentar baixar o dataset
    login(token=HF_TOKEN)
    
    ds = load_dataset(
        "WillianAlencar/SegmentacaoClientes",
        split="train"
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
    
    # Adicionando Métricas para facilitar a visualização
    c1, c2, c3 = st.columns(3)
    total_clientes = len(df)
    ja_compraram = len(df[df["status_compra"] == "Já comprou"])
    
    c1.metric("Total de Clientes", total_clientes)
    c2.metric("Já Compraram", ja_compraram)
    c3.metric("Nunca Compraram", total_clientes - ja_compraram)

    st.markdown("---")

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

    status_sel = st.sidebar.multiselect(
        "Status de Compra",
        options=["Nunca comprou", "Já comprou"],
        default=["Nunca comprou", "Já comprou"]
    )

    # ================================
    # FILTROS DE DATA
    # ================================
    st.sidebar.subheader("📅 Datas")

    data_visita = st.sidebar.date_input(
        "Período da Última Visita",
        value=(df["data_ultima_visita"].min(), df["data_ultima_visita"].max())
    )

    df_com_compra = df[df["data_ultima_compra"].notna()]
    if not df_com_compra.empty:
        data_compra = st.sidebar.date_input(
            "Período da Última Compra",
            value=(df_com_compra["data_ultima_compra"].min(), df_com_compra["data_ultima_compra"].max())
        )
    else:
        data_compra = None

    # ================================
    # APLICAÇÃO DOS FILTROS
    # ================================
    # Filtro base
    df_filtrado = df[
        (df["categoria"].isin(categorias)) &
        (df["setor"].isin(setores)) &
        (df["status_compra"].isin(status_sel)) &
        (df["data_ultima_visita"].dt.date.between(*data_visita))
    ]

    # Filtro de data de compra (se houver seleção e clientes que compraram)
    if data_compra and "Já comprou" in status_sel:
        mask_compra = df_filtrado["data_ultima_compra"].dt.date.between(*data_compra)
        mask_nunca = df_filtrado["status_compra"] == "Nunca comprou"
        df_filtrado = df_filtrado[mask_compra | mask_nunca]

    # ================================
    # TABELA FINAL E EXPORTAÇÃO
    # ================================
    st.subheader(f"📊 Membros Filtrados ({len(df_filtrado)})")
    st.dataframe(df_filtrado.reset_index(drop=True), use_container_width=True)

    # Botão de Download
    csv = df_filtrado.to_csv(index=False).encode('utf-8')
    st.download_button(
        label="📥 Baixar Dados Filtrados (CSV)",
        data=csv,
        file_name='membros_filtrados.csv',
        mime='text/csv',
    )

except Exception as e:
    st.error(f"❌ Erro ao carregar os dados: {e}")
    st.info("Verifique se o Token do Hugging Face tem permissão de LEITURA (READ) para este dataset.")
