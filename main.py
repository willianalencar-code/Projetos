import streamlit as st
import pandas as pd
from datasets import load_dataset
from huggingface_hub import login

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
# No Streamlit Cloud, vá em Settings > Secrets e adicione: HF_TOKEN = "seu_token"
# O código abaixo tenta pegar dos Secrets, se não achar, usa a string direta.
try:
    HF_TOKEN = st.secrets["HF_TOKEN"]
except:
    HF_TOKEN = "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"

# ================================
# FUNÇÃO PARA CARREGAR DADOS
# ================================
@st.cache_data(show_spinner="Autenticando e carregando dataset...")
def carregar_dados():
    # Realiza o login no Hugging Face (essencial para datasets privados)
    login(token=HF_TOKEN)
    
    # Carrega o dataset do Hub
    ds = load_dataset(
        "WillianAlencar/SegmentacaoClientes",
        split="train"
    )

    df = ds.to_pandas()

    # Conversão de datas: garante que sejam objetos datetime do Pandas
    for col in ["data_ultima_visita", "data_ultima_compra"]:
        df[col] = pd.to_datetime(df[col], errors="coerce")

    # Criação do Status de compra
    df["status_compra"] = df["data_ultima_compra"].isna().map(
        {True: "Nunca comprou", False: "Já comprou"}
    )

    return df

# ================================
# CARREGAMENTO DOS DADOS
# ================================
try:
    df = carregar_dados()

    # TÍTULO PRINCIPAL
    st.title("📂 Sistema Profissional de Filtro e Exportação")

    # MÉTRICAS RÁPIDAS
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
    st.sidebar.header("🔎 Filtros de Segmentação")

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

    # FILTROS DE DATA
    st.sidebar.subheader("📅 Filtros de Datas")

    # Última Visita (Tratando datetime para date para o widget)
    min_visita = df["data_ultima_visita"].min().date()
    max_visita = df["data_ultima_visita"].max().date()
    
    data_visita = st.sidebar.date_input(
        "Período da Última Visita",
        value=(min_visita, max_visita),
        min_value=min_visita,
        max_value=max_visita
    )

    # Última Compra (apenas para quem tem data)
    df_com_compra = df[df["data_ultima_compra"].notna()]
    if not df_com_compra.empty:
        min_compra = df_com_compra["data_ultima_compra"].min().date()
        max_compra = df_com_compra["data_ultima_compra"].max().date()
        
        data_compra_sel = st.sidebar.date_input(
            "Período da Última Compra",
            value=(min_compra, max_compra),
            min_value=min_compra,
            max_value=max_compra
        )
    else:
        data_compra_sel = None

    # ================================
    # APLICAÇÃO DOS FILTROS (LÓGICA)
    # ================================
    
    # Aplicando filtros de texto e o de data de visita (usando .dt.date para comparar)
    mask = (
        (df["categoria"].isin(categorias)) &
        (df["setor"].isin(setores)) &
        (df["status_compra"].isin(status_sel)) &
        (df["data_ultima_visita"].dt.date.between(*data_visita))
    )
    
    df_filtrado = df[mask].copy()

    # Aplicando filtro de data de compra se houver seleção
    if data_compra_sel and "Já comprou" in status_sel:
        # Mantém quem está no intervalo OU quem nunca comprou (se "Nunca comprou" estiver selecionado)
        mask_compra = (
            (df_filtrado["data_ultima_compra"].dt.date.between(*data_compra_sel)) |
            (df_filtrado["status_compra"] == "Nunca comprou")
        )
        df_filtrado = df_filtrado[mask_compra]

    # ================================
    # TABELA FINAL E EXPORTAÇÃO
    # ================================
    st.subheader(f"📊 Membros Filtrados ({len(df_filtrado)})")
    
    # Exibindo a tabela
    st.dataframe(
        df_filtrado.reset_index(drop=True), 
        use_container_width=True
    )

    # Botão de Exportação
    if not df_filtrado.empty:
        csv = df_filtrado.to_csv(index=False).encode('utf-8')
        st.download_button(
            label="📥 Baixar Base Filtrada (CSV)",
            data=csv,
            file_name='membros_filtrados.csv',
            mime='text/csv',
        )
    else:
        st.warning("Nenhum membro encontrado com os filtros selecionados.")

except Exception as e:
    st.error(f"❌ Erro na aplicação: {e}")
    st.info("Verifique se as bibliotecas 'datasets' e 'huggingface_hub' estão no requirements.txt")
