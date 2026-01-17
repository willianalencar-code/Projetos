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
# CARGA DE DADOS
# ================================
@st.cache_data
def carregar_dados():
    df = pd.read_csv("dataset.csv")

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

    # ================================
    # APLICAÇÃO DOS FILTROS
    # ================================
    mask = (
        df["categoria"].isin(categorias)
        & df["setor"].isin(setores)
        & df["status_compra"].isin(status_compra)
    )

    # Filtro de visita
    if isinstance(data_visita, tuple) and len(data_visita) == 2:
        mask &= (
            df["data_ultima_visita"].dt.date >= data_visita[0]
        ) & (
            df["data_ultima_visita"].dt.date <= data_visita[1]
        )

    # Filtro de compra (aplicado apenas a quem comprou)
    if data_compra and isinstance(data_compra, tuple) and len(data_compra) == 2:
        mask &= (
            (df["data_ultima_compra"].isna()) |
            (
                (df["data_ultima_compra"].dt.date >= data_compra[0]) &
                (df["data_ultima_compra"].dt.date <= data_compra[1])
            )
        )

    df_filtrado = df[mask]

    # ================================
    # MÉTRICAS
    # ================================
    col1, col2, col3, col4 = st.columns(4)

    col1.metric(
        "Membros Únicos",
        f"{df_filtrado['member_pk'].nunique():,}".replace(",", ".")
    )

    col2.metric(
        "Total de Registros",
        f"{len(df_filtrado):,}".replace(",", ".")
    )

    col3.metric(
        "Já Compraram",
        f"{(df_filtrado['status_compra'] == 'Já comprou').sum():,}".replace(",", ".")
    )

    col4.metric(
        "Nunca Compraram",
        f"{(df_filtrado['status_compra'] == 'Nunca comprou').sum():,}".replace(",", ".")
    )

    # ================================
    # QUALIDADE DOS DADOS
    # ================================
    st.subheader("🧪 Qualidade dos Dados")

    perc_sem_compra = (
        (df_filtrado["status_compra"] == "Nunca comprou").mean() * 100
        if len(df_filtrado) > 0 else 0
    )

    st.info(
        f"📌 {perc_sem_compra:.1f}% dos registros filtrados **não possuem compra registrada**."
    )

    # ================================
    # VISUALIZAÇÃO
    # ================================
    st.subheader("📊 Dados Filtrados")

    st.dataframe(
        df_filtrado,
        use_container_width=True,
        height=450
    )

    # ================================
    # EXPORTAÇÃO
    # ================================
    st.divider()

    st.warning(
        f"O arquivo exportado conterá {len(df_filtrado):,} registros."
        .replace(",", ".")
    )

    csv = df_filtrado.to_csv(index=False).encode("utf-8")

    st.download_button(
        label="📥 Exportar CSV",
        data=csv,
        file_name="base_filtrada.csv",
        mime="text/csv"
    )

except FileNotFoundError:
    st.error("Arquivo dataset.csv não encontrado.")
except Exception as e:
    st.error(f"Erro inesperado: {e}")
