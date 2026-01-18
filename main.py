import streamlit as st
import pandas as pd

# ================================
# CONFIGURAÇÃO DA PÁGINA
# ================================
st.set_page_config(
    page_title="Sistema de Filtro e Exportação de Membros",
    layout="wide"
)

# ================================
# URL DO CSV NO HUGGING FACE
# ================================
# Você precisa pegar o CSV bruto do seu dataset no Hugging Face
HF_CSV_URL = "https://huggingface.co/datasets/WillianAlencar/SegmentacaoClientes/resolve/main/train.csv"

# ================================
# CARGA DE DADOS
# ================================
@st.cache_data(show_spinner="Carregando dataset...")
def carregar_dados():
    df = pd.read_csv(HF_CSV_URL)

    # Conversão de datas
    df["data_ultima_visita"] = pd.to_datetime(df["data_ultima_visita"], errors="coerce")
    df["data_ultima_compra"] = pd.to_datetime(df["data_ultima_compra"], errors="coerce")

    # Criação de STATUS DE COMPRA
    df["status_compra"] = df["data_ultima_compra"].isna().map(
        {True: "Nunca comprou", False: "Já comprou"}
    )

    return df

# ================================
# CARREGAMENTO E FILTROS
# ================================
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

    # ================================
    # APLICAÇÃO DOS FILTROS
    # ================================
    df_filtrado = df[
        (df["categoria"].isin(categorias)) &
        (df["setor"].isin(setores)) &
        (df["status_compra"].isin(status_compra))
    ]

    # Filtro por datas
    df_filtrado = df_filtrado[
        (df_filtrado["data_ultima_visita"].between(data_visita[0], data_visita[1]))
    ]

    if data_compra:
        df_filtrado = df_filtrado[
            (df_filtrado["data_ultima_compra"].between(data_compra[0], data_compra[1]))
            | (df_filtrado["status_compra"] == "Nunca comprou")
        ]

    # ================================
    # TABELA FINAL
    # ================================
    st.subheader(f"📊 Membros Filtrados ({len(df_filtrado)})")
    st.dataframe(df_filtrado.reset_index(drop=True))

except Exception as e:
    st.error(f"❌ Erro ao carregar os dados: {e}")
