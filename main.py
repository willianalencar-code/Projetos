import streamlit as st
import duckdb
from huggingface_hub import hf_hub_download
import pandas as pd
import os
import tempfile
from datetime import datetime

# ==========================================
# CONFIGURAÇÃO DA PÁGINA
# ==========================================
st.set_page_config(
    page_title="Exportador de Clientes 7M",
    layout="wide",
    page_icon="🚀"
)

# ==========================================
# CACHE E CONEXÃO
# ==========================================
@st.cache_data(show_spinner=False)
def get_dataset():
    try:
        token = st.secrets["HF_TOKEN"]
        caminho_local = hf_hub_download(
            repo_id="WillianAlencar/SegmentacaoClientes",
            filename="data/train-00000-of-00001.parquet",
            repo_type="dataset",
            token=token
        )
        return caminho_local
    except Exception as e:
        st.error(f"Erro de conexão com o Hugging Face: {e}")
        return None

@st.cache_resource(show_spinner=False)
def get_connection():
    return duckdb.connect(database=':memory:')

# ==========================================
# INICIALIZAÇÃO
# ==========================================
caminho_arquivo = get_dataset()
con = get_connection()

if caminho_arquivo:
    con.execute(f"CREATE OR REPLACE TABLE clientes AS SELECT * FROM read_parquet('{caminho_arquivo}')")
    st.title("🚀 Exportador de Dados - 7 Milhões de Clientes")

    # ==========================================
    # SIDEBAR - FILTROS
    # ==========================================
    st.sidebar.header("🔍 Filtros")

    # member_pk
    id_busca = st.sidebar.text_input("Buscar por member_pk:", key="id_busca")

    # categorias
    categorias = con.execute("SELECT DISTINCT categoria FROM clientes WHERE categoria IS NOT NULL").df()['categoria'].tolist()
    cat_sel = st.sidebar.multiselect("Categorias:", categorias, key="cat_sel")

    # setores
    setores = con.execute("SELECT DISTINCT setor FROM clientes WHERE setor IS NOT NULL").df()['setor'].tolist()
    setor_sel = st.sidebar.multiselect("Setores:", setores, key="setor_sel")

    # datas
    min_visita, max_visita = con.execute("SELECT MIN(data_ultima_visita), MAX(data_ultima_visita) FROM clientes").fetchone()
    min_compra, max_compra = con.execute("SELECT MIN(data_ultima_compra), MAX(data_ultima_compra) FROM clientes").fetchone()

    # session_state para última visita
    if "date_visita" not in st.session_state:
        st.session_state.date_visita = [min_visita, max_visita]

    # session_state para última compra
    if "date_compra" not in st.session_state:
        st.session_state.date_compra = [min_compra, max_compra]
        st.session_state.date_compra_selected = False  # não filtrando por default

    # Widget para última visita
    date_visita_range = st.sidebar.date_input(
        "Período da última visita",
        value=st.session_state.date_visita,
        key="date_visita_input"
    )

    # Widget para última compra como range
    date_compra_range = st.sidebar.date_input(
        "Período da última compra",
        value=st.session_state.date_compra if st.session_state.date_compra_selected else [min_compra, max_compra],
        key="date_compra_input"
    )

    # Atualiza session_state
    st.session_state.date_visita = date_visita_range
    if date_compra_range != [min_compra, max_compra]:
        st.session_state.date_compra_selected = True
        st.session_state.date_compra = date_compra_range
    else:
        st.session_state.date_compra_selected = False

    # opção somente member_pk
    only_member_pk = st.sidebar.checkbox("Exportar apenas member_pk", value=False)

    # formato (Excel e CSV apenas)
    export_format = st.sidebar.selectbox("Formato:", ["Excel (.xlsx)", "CSV"])

    # ==========================================
    # MONTAR QUERY
    # ==========================================
    query = "SELECT * FROM clientes WHERE 1=1"
    if id_busca:
        query += f" AND CAST(member_pk AS VARCHAR) LIKE '%{id_busca}%'"
    if cat_sel:
        query += f" AND categoria IN ({', '.join([f'\'{c}\'' for c in cat_sel])})"
    if setor_sel:
        query += f" AND setor IN ({', '.join([f'\'{s}\'' for s in setor_sel])})"
    if len(date_visita_range) == 2:
        query += f" AND data_ultima_visita BETWEEN '{date_visita_range[0]}' AND '{date_visita_range[1]}'"
    if st.session_state.date_compra_selected and len(st.session_state.date_compra) == 2:
        start, end = st.session_state.date_compra
        query += f" AND data_ultima_compra BETWEEN '{start}' AND '{end}'"

    if only_member_pk:
        query = query.replace("SELECT *", "SELECT member_pk")

    # ==========================================
    # METRICS
    # ==========================================
    total = con.execute(f"SELECT COUNT(*) FROM ({query})").fetchone()[0]
    total_unicos = con.execute(f"SELECT COUNT(DISTINCT member_pk) FROM ({query})").fetchone()[0]

    col1, col2, col3 = st.columns(3)
    col1.metric("Total de registros", f"{total:,}")
    col2.metric("Clientes únicos (member_pk)", f"{total_unicos:,}")
    col3.metric("Base de dados", "Hugging Face")

    # ==========================================
    # PRÉ-VISUALIZAÇÃO
    # ==========================================
    st.subheader("📋 Pré-visualização (100 linhas)")
    df_preview = con.execute(query + " LIMIT 100").df()
    for col in ['data_ultima_visita', 'data_ultima_compra']:
        if col in df_preview.columns:
            df_preview[col] = pd.to_datetime(df_preview[col], errors='coerce')
    st.dataframe(df_preview, use_container_width=True)

    # ==========================================
    # EXPORTAÇÃO
    # ==========================================
    st.header("📤 Exportação")
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    file_ext = "xlsx" if export_format.startswith("Excel") else "csv"
    file_name = f"clientes_{timestamp}.{file_ext}"

    st.info(f"Nome do arquivo: **{file_name}**")
    st.info(f"Total estimado para exportação: {total:,} registros")

    if st.button("🚀 INICIAR EXPORTAÇÃO", type="primary", use_container_width=True):
        with st.spinner(f"Exportando {total:,} registros..."):
            try:
                tmp_file = tempfile.NamedTemporaryFile(delete=False, suffix=f".{file_ext}")
                tmp_path = tmp_file.name
                tmp_file.close()

                df_export = con.execute(query).df()

                if export_format.startswith("Excel"):
                    df_export.to_excel(tmp_path, index=False)
                    mime_type = "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
                else:
                    df_export.to_csv(tmp_path, index=False)
                    mime_type = "text/csv"

                file_size = os.path.getsize(tmp_path) / (1024*1024)
                with open(tmp_path, 'rb') as f:
                    file_data = f.read()
                os.unlink(tmp_path)

                st.success(f"✅ Exportação concluída! Tamanho: {file_size:.2f} MB")
                st.download_button(
                    label=f"📥 BAIXAR ARQUIVO ({file_size:.2f} MB)",
                    data=file_data,
                    file_name=file_name,
                    mime=mime_type,
                    use_container_width=True
                )

            except Exception as e:
                st.error(f"❌ Erro durante exportação: {e}")

else:
    st.warning("Aguardando configuração do Token HF_TOKEN nos secrets do Streamlit Cloud.")
