import streamlit as st
import duckdb
from huggingface_hub import hf_hub_download
import pandas as pd
import os
import tempfile
from datetime import datetime

# ==========================================
# CONFIGURAÇÃO
# ==========================================
st.set_page_config(
    page_title="Exportador de Clientes 7M",
    layout="wide",
    page_icon="🚀"
)

@st.cache_data(show_spinner=False)
def get_dataset():
    """Baixa o arquivo do HF"""
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
        st.error(f"Erro: {e}")
        return None

@st.cache_resource(show_spinner=False)
def get_connection():
    return duckdb.connect(database=':memory:')

# ==========================================
# FUNÇÕES DE EXPORTAÇÃO OTIMIZADAS
# ==========================================
def export_chunked(con, query, chunk_size=500000, format="csv"):
    """Exporta dados em pedaços"""
    total_query = f"SELECT COUNT(*) FROM ({query})"
    total_count = con.execute(total_query).fetchone()[0]
    
    num_chunks = (total_count // chunk_size) + (1 if total_count % chunk_size > 0 else 0)
    
    results = []
    for i in range(num_chunks):
        offset = i * chunk_size
        chunk_query = f"SELECT * FROM ({query}) LIMIT {chunk_size} OFFSET {offset}"
        df_chunk = con.execute(chunk_query).df()
        results.append(df_chunk)
        
        # Progresso
        progress = (i + 1) / num_chunks
        st.progress(progress, text=f"Processando pedaço {i+1}/{num_chunks}")
    
    if results:
        return pd.concat(results, ignore_index=True)
    return pd.DataFrame()

# ==========================================
# INTERFACE PRINCIPAL
# ==========================================
caminho_arquivo = get_dataset()
con = get_connection()

if caminho_arquivo:
    con.execute(f"CREATE OR REPLACE TABLE clientes AS SELECT * FROM read_parquet('{caminho_arquivo}')")
    
    st.title("🚀 Exportador de Dados - 7 Milhões de Clientes")
    
    # Sidebar
    st.sidebar.header("⚙️ Configurações de Exportação")
    
    # Opções principais
    export_mode = st.sidebar.radio(
        "Modo de exportação:",
        ["💡 Amostra Rápida", "📊 Dados Filtrados", "🚀 Dataset Completo (7M)"],
        help="Amostra: 100K linhas | Filtrados: Aplicando filtros | Completo: Todos os 7M"
    )
    
    # Filtros (apenas para modo filtrado)
    cat_sel, setor_sel = [], []
    if export_mode == "📊 Dados Filtrados":
        st.sidebar.subheader("🔍 Filtros")
        
        categorias = con.execute("SELECT DISTINCT categoria FROM clientes LIMIT 50").df()['categoria'].tolist()
        cat_sel = st.sidebar.multiselect("Categorias:", categorias)
        
        setores = con.execute("SELECT DISTINCT setor FROM clientes LIMIT 50").df()['setor'].tolist()
        setor_sel = st.sidebar.multiselect("Setores:", setores)
    
    # Filtro de datas
    st.sidebar.subheader("🔍 Filtro de Data")
    min_date, max_date = con.execute("SELECT MIN(created_at), MAX(created_at) FROM clientes").fetchone()
    if min_date and max_date:
        date_range = st.sidebar.date_input(
            "Período",
            value=[min_date, max_date],
            min_value=min_date,
            max_value=max_date
        )
    else:
        date_range = None
    
    # Exportar apenas member_pk
    only_member_pk = st.sidebar.checkbox("Exportar apenas member_pk", value=False, help="Somente a coluna de identificação")
    
    # Formato
    export_format = st.sidebar.selectbox(
        "Formato:",
        ["Parquet (Recomendado)", "CSV", "JSON"],
        help="Parquet: Mais rápido e menor | CSV: Universal | JSON: Para APIs"
    )
    
    # Opções avançadas
    with st.sidebar.expander("⚡ Opções Avançadas"):
        use_chunks = st.checkbox("Dividir em partes", value=True, help="Divide a exportação para evitar travamentos")
        if use_chunks:
            chunk_size = st.select_slider(
                "Registros por parte:",
                options=[100000, 250000, 500000, 1000000],
                value=500000
            )
        compress = st.checkbox("Comprimir arquivo", value=True, help="Reduz tamanho do arquivo (especialmente útil para CSV/JSON)")
    
    # Informações gerais
    total_clientes = con.execute("SELECT COUNT(*) FROM clientes").fetchone()[0]
    st.sidebar.info(f"**Total na base:** {total_clientes:,} clientes")
    
    # ==========================================
    # PRÉ-VISUALIZAÇÃO
    # ==========================================
    st.header("📋 Pré-visualização")
    
    col1, col2, col3 = st.columns(3)
    with col1:
        st.metric("Clientes Totais", f"{total_clientes:,}")
    with col2:
        st.metric("Modo Selecionado", export_mode.split()[0])
    with col3:
        st.metric("Formato", export_format.split()[0])
    
    st.subheader("👁️ Amostra dos Dados (100 primeiros)")
    sample_df = con.execute("SELECT * FROM clientes LIMIT 100").df()
    st.dataframe(sample_df, use_container_width=True)
    
    # ==========================================
    # PREPARAR QUERY DE EXPORTAÇÃO
    # ==========================================
    if export_mode == "💡 Amostra Rápida":
        base_query = "SELECT * FROM clientes LIMIT 100000"
        estimated_rows = 100000
    elif export_mode == "📊 Dados Filtrados":
        base_query = "SELECT * FROM clientes WHERE 1=1"
        if cat_sel:
            cat_values = ", ".join([f"'{c}'" for c in cat_sel])
            base_query += f" AND categoria IN ({cat_values})"
        if setor_sel:
            setor_values = ", ".join([f"'{s}'" for s in setor_sel])
            base_query += f" AND setor IN ({setor_values})"
        if date_range:
            start_date = date_range[0].strftime('%Y-%m-%d')
            end_date = date_range[1].strftime('%Y-%m-%d')
            base_query += f" AND created_at BETWEEN '{start_date}' AND '{end_date}'"
        try:
            estimated_rows = con.execute(f"SELECT COUNT(*) FROM ({base_query})").fetchone()[0]
        except:
            estimated_rows = con.execute("SELECT COUNT(*) FROM clientes").fetchone()[0]
    else:  # Dataset Completo
        base_query = "SELECT * FROM clientes"
        if date_range:
            start_date = date_range[0].strftime('%Y-%m-%d')
            end_date = date_range[1].strftime('%Y-%m-%d')
            base_query += f" AND created_at BETWEEN '{start_date}' AND '{end_date}'"
        estimated_rows = total_clientes
    
    # Aplicar somente member_pk
    if only_member_pk:
        base_query = base_query.replace("SELECT *", "SELECT member_pk")
    
    # Contagem de member_pk único
    unique_members_filtered = con.execute(f"SELECT COUNT(DISTINCT member_pk) FROM ({base_query})").fetchone()[0]
    st.info(f"Clientes únicos na exportação: **{unique_members_filtered:,}**")
    
    # Nome do arquivo
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    file_ext_preview = export_format.split()[0].lower()
    file_name_preview = f"clientes_{export_mode.split()[0].lower()}_{timestamp}.{file_ext_preview}"
    st.info(f"Nome do arquivo que será gerado: **{file_name_preview}**")
    
    st.info(f"**📊 Estimativa:** {estimated_rows:,} registros serão exportados")
    
    # ==========================================
    # BOTÃO DE EXPORTAÇÃO
    # ==========================================
    if st.button("🚀 INICIAR EXPORTAÇÃO", type="primary", use_container_width=True):
        with st.spinner(f"Preparando exportação de {estimated_rows:,} registros..."):
            try:
                # Criar arquivo temporário
                with tempfile.NamedTemporaryFile(delete=False, suffix=f".{export_format.split()[0].lower()}") as tmp_file:
                    tmp_path = tmp_file.name
                
                # Exportar usando DuckDB direto
                if export_format == "Parquet (Recomendado)":
                    con.execute(f"COPY ({base_query}) TO '{tmp_path}' (FORMAT PARQUET)")
                    mime_type = "application/octet-stream"
                    file_ext = "parquet"
                elif export_format == "CSV":
                    if compress:
                        con.execute(f"COPY ({base_query}) TO '{tmp_path}' (FORMAT CSV, HEADER true, COMPRESSION GZIP)")
                        file_ext = "csv.gz"
                        mime_type = "application/gzip"
                    else:
                        con.execute(f"COPY ({base_query}) TO '{tmp_path}' (FORMAT CSV, HEADER true)")
                        file_ext = "csv"
                        mime_type = "text/csv"
                elif export_format == "JSON":
                    if use_chunks and estimated_rows > chunk_size:
                        df_export = export_chunked(con, base_query, chunk_size)
                    else:
                        df_export = con.execute(base_query).df()
                    if compress:
                        df_export.to_json(tmp_path, orient='records', lines=True, compression='gzip')
                        file_ext = "json.gz"
                        mime_type = "application/gzip"
                    else:
                        df_export.to_json(tmp_path, orient='records', lines=True)
                        file_ext = "json"
                        mime_type = "application/json"
                
                # Ler arquivo gerado
                file_size = os.path.getsize(tmp_path) / (1024 * 1024)  # MB
                with open(tmp_path, 'rb') as f:
                    file_data = f.read()
                
                os.unlink(tmp_path)
                
                st.success(f"✅ Exportação concluída! Arquivo: {file_size:.2f} MB")
                
                # Botão de download
                filename = f"clientes_{export_mode.split()[0].lower()}_{timestamp}.{file_ext}"
                st.download_button(
                    label=f"📥 BAIXAR ARQUIVO ({file_size:.2f} MB)",
                    data=file_data,
                    file_name=filename,
                    mime=mime_type,
                    use_container_width=True,
                    type="primary"
                )
                
                # Estatísticas
                with st.expander("📊 Estatísticas da Exportação"):
                    col_stat1, col_stat2, col_stat3 = st.columns(3)
                    with col_stat1:
                        st.metric("Registros Exportados", f"{estimated_rows:,}")
                    with col_stat2:
                        st.metric("Tamanho do Arquivo", f"{file_size:.2f} MB")
                    with col_stat3:
                        compression_ratio = (estimated_rows * 100) / (file_size * 1024 * 1024) if file_size > 0 else 0
                        st.metric("Taxa Compressão", f"{compression_ratio:.1f} bytes/registro")
                
            except Exception as e:
                st.error(f"❌ Erro durante exportação: {str(e)}")
                with st.expander("🛠️ Soluções possíveis"):
                    st.markdown("""
                    **Se a exportação falhou:**
                    1. **Tente exportar em partes menores** - Use a opção "Dividir em partes"
                    2. **Use formato Parquet** - É mais eficiente que CSV
                    3. **Exporte apenas uma amostra** - 100K registros primeiro
                    4. **Verifique sua conexão** - 7M registros exigem boa conexão
                    5. **Tente novamente em alguns minutos** - Pode ser congestionamento temporário
                    """)
    
    # ==========================================
    # DICAS
    # ==========================================
    with st.expander("💡 Dicas para Exportação de Grandes Volumes"):
        st.markdown("""
        **Para 7 milhões de registros:**
        
        🥇 **Parquet é o MELHOR formato:**
        - 10x mais rápido que CSV
        - 5x menor em tamanho
        - Mantém tipos de dados
        
        ⚡ **Performance:**
        - Exportação completa: 2-5 minutos
        - Tamanho estimado: 200-500 MB (Parquet)
        - Tamanho CSV: 1-2 GB
        
        🛡️ **Segurança:**
        - Dados processados em memória
        - Arquivo temporário é apagado
        - Nenhum dado fica no servidor
        
        📱 **Como usar depois:**
        ```python
        # Para Parquet:
        import pandas as pd
        df = pd.read_parquet('arquivo.parquet')
        
        # Para CSV comprimido:
        df = pd.read_csv('arquivo.csv.gz')
        ```
        """)
else:
    st.warning("Configure o token HF_TOKEN nos secrets do Streamlit Cloud")
