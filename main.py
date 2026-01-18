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
# EXPORTAÇÃO EM CHUNKS
# ==========================================
def export_chunked(con, query, chunk_size=500000):
    total_count = con.execute(f"SELECT COUNT(*) FROM ({query})").fetchone()[0]
    num_chunks = (total_count // chunk_size) + (1 if total_count % chunk_size > 0 else 0)
    results = []
    for i in range(num_chunks):
        offset = i * chunk_size
        chunk_query = f"SELECT * FROM ({query}) LIMIT {chunk_size} OFFSET {offset}"
        df_chunk = con.execute(chunk_query).df()
        results.append(df_chunk)
        st.progress((i+1)/num_chunks, text=f"Processando chunk {i+1}/{num_chunks}")
    return pd.concat(results, ignore_index=True) if results else pd.DataFrame()

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

    # --- member_pk
    id_busca = st.sidebar.text_input("Buscar por member_pk:", key="id_busca")

    # --- categorias
    categorias = con.execute("SELECT DISTINCT categoria FROM clientes WHERE categoria IS NOT NULL").df()['categoria'].tolist()
    cat_sel = st.sidebar.multiselect("Categorias:", categorias, key="cat_sel")

    # --- setores
    setores = con.execute("SELECT DISTINCT setor FROM clientes WHERE setor IS NOT NULL").df()['setor'].tolist()
    setor_sel = st.sidebar.multiselect("Setores:", setores, key="setor_sel")

    # --- datas
    min_visita, max_visita = con.execute("SELECT MIN(data_ultima_visita), MAX(data_ultima_visita) FROM clientes").fetchone()
    min_compra, max_compra = con.execute("SELECT MIN(data_ultima_compra), MAX(data_ultima_compra) FROM clientes").fetchone()

    if "date_visita" not in st.session_state:
        st.session_state.date_visita = [min_visita, max_visita]
    if "date_compra" not in st.session_state:
        st.session_state.date_compra = [min_compra, max_compra]

    date_visita_range = st.sidebar.date_input("Período da última visita", value=st.session_state.date_visita, key="date_visita_input")
    date_compra_range = st.sidebar.date_input("Período da última compra", value=st.session_state.date_compra, key="date_compra_input")

    st.session_state.date_visita = date_visita_range
    st.session_state.date_compra = date_compra_range

    # --- opção somente member_pk
    only_member_pk = st.sidebar.checkbox("Exportar apenas member_pk", value=False)

    # --- formato e chunking
    export_format = st.sidebar.selectbox("Formato:", ["Parquet (Recomendado)", "CSV", "JSON"])
    use_chunks = st.sidebar.checkbox("Dividir em partes", value=True)
    if use_chunks:
        chunk_size = st.sidebar.select_slider("Registros por chunk:", options=[100000, 250000, 500000, 1000000], value=500000)
    compress = st.sidebar.checkbox("Comprimir arquivo (CSV/JSON)", value=True)

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
    if len(date_compra_range) == 2:
        query += f" AND data_ultima_compra BETWEEN '{date_compra_range[0]}' AND '{date_compra_range[1]}'"

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
    # EXPORTAÇÃO
    # ==========================================
    st.header("📤 Exportação")
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    file_ext_preview = export_format.split()[0].lower()
    file_name_preview = f"clientes_{timestamp}.{file_ext_preview}"
    st.info(f"Nome do arquivo que será gerado: **{file_name_preview}**")
    st.info(f"Total estimado para exportação: {total:,} registros")

    if st.button("🚀 INICIAR EXPORTAÇÃO", type="primary", use_container_width=True):
        with st.spinner(f"Exportando {total:,} registros..."):
            try:
                tmp_file = tempfile.NamedTemporaryFile(delete=False, suffix=f".{file_ext_preview}")
                tmp_path = tmp_file.name
                tmp_file.close()

                if export_format == "Parquet (Recomendado)":
                    con.execute(f"COPY ({query}) TO '{tmp_path}' (FORMAT PARQUET)")
                    mime_type = "application/octet-stream"
                    file_ext = "parquet"
                elif export_format == "CSV":
                    if compress:
                        con.execute(f"COPY ({query}) TO '{tmp_path}' (FORMAT CSV, HEADER true, COMPRESSION GZIP)")
                        file_ext = "csv.gz"
                        mime_type = "application/gzip"
                    else:
                        con.execute(f"COPY ({query}) TO '{tmp_path}' (FORMAT CSV, HEADER true)")
                        file_ext = "csv"
                        mime_type = "text/csv"
                elif export_format == "JSON":
                    if use_chunks and total > chunk_size:
                        df_export = export_chunked(con, query, chunk_size)
                    else:
                        df_export = con.execute(query).df()
                    if compress:
                        df_export.to_json(tmp_path, orient='records', lines=True, compression='gzip')
                        file_ext = "json.gz"
                        mime_type = "application/gzip"
                    else:
                        df_export.to_json(tmp_path, orient='records', lines=True)
                        file_ext = "json"
                        mime_type = "application/json"

                # Ler e disponibilizar para download
                file_size = os.path.getsize(tmp_path) / (1024*1024)
                with open(tmp_path, 'rb') as f:
                    file_data = f.read()
                os.unlink(tmp_path)

                st.success(f"✅ Exportação concluída! Tamanho: {file_size:.2f} MB")
                filename = f"clientes_{timestamp}.{file_ext}"
                st.download_button(
                    label=f"📥 BAIXAR ARQUIVO ({file_size:.2f} MB)",
                    data=file_data,
                    file_name=filename,
                    mime=mime_type,
                    use_container_width=True
                )

            except Exception as e:
                st.error(f"❌ Erro durante exportação: {e}")

else:
    st.warning("Aguardando configuração do Token HF_TOKEN nos secrets do Streamlit Cloud.")
