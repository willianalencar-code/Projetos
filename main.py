import streamlit as st
import duckdb
from huggingface_hub import hf_hub_download
import pandas as pd
import os
import tempfile
from datetime import datetime
import pyarrow.parquet as pq
import warnings
warnings.filterwarnings('ignore')

# ==========================================
# CONFIGURAÇÃO DA PÁGINA
# ==========================================
st.set_page_config(
    page_title="Exportador de Clientes 7M",
    layout="wide",
    page_icon="🚀"
)

# ==========================================
# CACHE E CONEXÃO OTIMIZADOS
# ==========================================
@st.cache_data(show_spinner=False, ttl=3600)
def get_dataset_info():
    """Obtém apenas informações do dataset sem carregar tudo na memória"""
    try:
        token = st.secrets.get("HF_TOKEN", "")
        caminho_local = hf_hub_download(
            repo_id="WillianAlencar/SegmentacaoClientes",
            filename="dataset.parquet",
            repo_type="dataset",
            token=token if token else None
        )
        
        # Lê apenas os metadados do arquivo Parquet
        parquet_file = pq.ParquetFile(caminho_local)
        num_rows = parquet_file.metadata.num_rows
        
        # Lê apenas uma amostra para obter colunas
        sample = pd.read_parquet(caminho_local, columns=['categoria', 'setor'], nrows=1000)
        
        categorias = sample['categoria'].dropna().unique().tolist() if 'categoria' in sample.columns else []
        setores = sample['setor'].dropna().unique().tolist() if 'setor' in sample.columns else []
        
        return {
            'caminho': caminho_local,
            'num_rows': num_rows,
            'categorias': sorted(categorias),
            'setores': sorted(setores)
        }
    except Exception as e:
        st.error(f"Erro de conexão: {e}")
        return None

@st.cache_resource(show_spinner=False)
def get_connection():
    return duckdb.connect(database=':memory:')

# ==========================================
# INICIALIZAÇÃO OTIMIZADA
# ==========================================
st.title("📋 Gestão e Exportação de Clientes")

# Carrega apenas informações do dataset
dataset_info = get_dataset_info()

if dataset_info:
    # ==========================================
    # SIDEBAR - FILTROS
    # ==========================================
    st.sidebar.header("🔍 Filtros")
    
    # member_pk
    id_busca = st.sidebar.text_input("Buscar por member_pk:", key="id_busca")
    
    # categorias
    categorias = dataset_info['categorias']
    cat_sel = st.sidebar.multiselect("Categorias:", categorias, key="cat_sel")
    
    # setores
    setores = dataset_info['setores']
    setor_sel = st.sidebar.multiselect("Setores:", setores, key="setor_sel")
    
    # Filtros de data com valores padrão
    col1, col2 = st.sidebar.columns(2)
    
    with col1:
        data_inicio_visita = st.date_input(
            "Data início visita",
            value=datetime(2020, 1, 1),
            key="data_inicio_visita"
        )
    
    with col2:
        data_fim_visita = st.date_input(
            "Data fim visita",
            value=datetime.now(),
            key="data_fim_visita"
        )
    
    col3, col4 = st.sidebar.columns(2)
    
    with col3:
        data_inicio_compra = st.date_input(
            "Data início compra",
            value=datetime(2020, 1, 1),
            key="data_inicio_compra"
        )
    
    with col4:
        data_fim_compra = st.date_input(
            "Data fim compra",
            value=datetime.now(),
            key="data_fim_compra"
        )
    
    # opção somente member_pk
    only_member_pk = st.sidebar.checkbox("Exportar apenas member_pk", value=False)
    
    # ==========================================
    # CONSTRUÇÃO DA QUERY COM PROCESSAMENTO EM LOTES
    # ==========================================
    def build_query_conditions():
        conditions = []
        
        if id_busca:
            conditions.append(f"CAST(member_pk AS VARCHAR) = '{id_busca}'")
        
        if cat_sel:
            cat_list = ', '.join([f"'{c}'" for c in cat_sel])
            conditions.append(f"categoria IN ({cat_list})")
        
        if setor_sel:
            setor_list = ', '.join([f"'{s}'" for s in setor_sel])
            conditions.append(f"setor IN ({setor_list})")
        
        conditions.append(f"data_ultima_visita BETWEEN '{data_inicio_visita}' AND '{data_fim_visita}'")
        conditions.append(f"data_ultima_compra BETWEEN '{data_inicio_compra}' AND '{data_fim_compra}'")
        
        return conditions
    
    # ==========================================
    # ESTIMATIVA DE RESULTADOS
    # ==========================================
    st.sidebar.header("📊 Estatísticas")
    
    # Estimativa otimizada
    conditions = build_query_conditions()
    where_clause = " AND ".join(conditions) if conditions else "1=1"
    
    # Query para contar resultados (usando DuckDB direto no arquivo)
    count_query = f"""
    SELECT COUNT(*) as total
    FROM read_parquet('{dataset_info['caminho']}')
    WHERE {where_clause}
    """
    
    try:
        con = get_connection()
        total_filtrado = con.execute(count_query).fetchone()[0]
        
        # Ajusta formato de exportação baseado no tamanho
        if total_filtrado > 500000:
            export_format = "CSV"
            st.sidebar.warning(f"⚠️ {total_filtrado:,} registros - Usando CSV para melhor desempenho")
        else:
            export_format = st.sidebar.selectbox(
                "Formato de exportação:",
                ["CSV", "Excel (.xlsx)"],
                key="export_format"
            )
        
        st.sidebar.metric("Registros filtrados", f"{total_filtrado:,}")
        st.sidebar.metric("Total no dataset", f"{dataset_info['num_rows']:,}")
        
    except Exception as e:
        st.sidebar.error(f"Erro na contagem: {e}")
        total_filtrado = 0
        export_format = "CSV"
    
    # ==========================================
    # MÉTRICAS PRINCIPAIS
    # ==========================================
    col1, col2, col3 = st.columns(3)
    col1.metric("Total no dataset", f"{dataset_info['num_rows']:,}")
    col2.metric("Estimativa filtrada", f"{total_filtrado:,}")
    col3.metric("Formato", export_format)
    
    # ==========================================
    # PRÉ-VISUALIZAÇÃO OTIMIZADA
    # ==========================================
    st.subheader("📋 Pré-visualização (50 linhas)")
    
    # Query para preview otimizada
    select_cols = "member_pk" if only_member_pk else "*"
    preview_query = f"""
    SELECT {select_cols}
    FROM read_parquet('{dataset_info['caminho']}')
    WHERE {where_clause}
    LIMIT 50
    """
    
    try:
        df_preview = con.execute(preview_query).df()
        
        if not df_preview.empty:
            # Formata colunas de data
            date_cols = ['data_ultima_visita', 'data_ultima_compra']
            for col in date_cols:
                if col in df_preview.columns:
                    df_preview[col] = pd.to_datetime(df_preview[col], errors='coerce').dt.date
            
            st.dataframe(df_preview, use_container_width=True)
        else:
            st.info("Nenhum resultado encontrado com os filtros atuais.")
            
    except Exception as e:
        st.error(f"Erro na pré-visualização: {e}")
    
    # ==========================================
    # EXPORTAÇÃO COM PROCESSAMENTO EM LOTES
    # ==========================================
    st.header("📤 Exportação")
    
    if total_filtrado > 0:
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        file_ext = "xlsx" if export_format == "Excel (.xlsx)" else "csv"
        file_name = f"clientes_{timestamp}.{file_ext}"
        
        st.info(f"📄 Nome do arquivo: **{file_name}**")
        st.info(f"📊 Total para exportação: **{total_filtrado:,}** registros")
        
        # Limite para Excel
        if total_filtrado > 1000000 and export_format == "Excel (.xlsx)":
            st.error("❌ Não é possível exportar mais de 1 milhão de registros em Excel.")
            st.error("Por favor, altere o formato para CSV.")
        else:
            if st.button("🚀 INICIAR EXPORTAÇÃO", type="primary", use_container_width=True):
                progress_bar = st.progress(0)
                status_text = st.empty()
                
                try:
                    # Cria arquivo temporário
                    tmp_file = tempfile.NamedTemporaryFile(delete=False, suffix=f".{file_ext}")
                    tmp_path = tmp_file.name
                    tmp_file.close()
                    
                    # Processa em lotes para CSV (mais eficiente para grandes volumes)
                    if export_format == "CSV":
                        status_text.text("Exportando em lotes...")
                        
                        # Configura lote
                        batch_size = 100000
                        first_batch = True
                        
                        # Query para exportação
                        select_cols = "member_pk" if only_member_pk else "*"
                        export_query = f"""
                        SELECT {select_cols}
                        FROM read_parquet('{dataset_info['caminho']}')
                        WHERE {where_clause}
                        """
                        
                        # Exporta em lotes
                        for i, batch in enumerate(con.execute(export_query).fetch_df_chunk(batch_size)):
                            if first_batch:
                                batch.to_csv(tmp_path, index=False)
                                first_batch = False
                            else:
                                batch.to_csv(tmp_path, mode='a', header=False, index=False)
                            
                            progress = min((i + 1) * batch_size / total_filtrado, 1.0)
                            progress_bar.progress(progress)
                            status_text.text(f"Processando lote {i+1}... ({len(batch):,} registros)")
                    
                    # Processa tudo de uma vez para Excel (apenas para volumes menores)
                    else:
                        status_text.text("Exportando para Excel...")
                        
                        select_cols = "member_pk" if only_member_pk else "*"
                        export_query = f"""
                        SELECT {select_cols}
                        FROM read_parquet('{dataset_info['caminho']}')
                        WHERE {where_clause}
                        """
                        
                        df_export = con.execute(export_query).df()
                        df_export.to_excel(tmp_path, index=False)
                        progress_bar.progress(1.0)
                    
                    # Obtém tamanho do arquivo
                    file_size = os.path.getsize(tmp_path) / (1024*1024)
                    
                    # Prepara download
                    with open(tmp_path, 'rb') as f:
                        file_data = f.read()
                    
                    os.unlink(tmp_path)
                    
                    progress_bar.empty()
                    status_text.empty()
                    
                    st.success(f"✅ Exportação concluída! Tamanho: {file_size:.2f} MB")
                    
                    # Botão de download
                    mime_type = "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet" if export_format == "Excel (.xlsx)" else "text/csv"
                    
                    st.download_button(
                        label=f"📥 BAIXAR ARQUIVO ({file_size:.2f} MB)",
                        data=file_data,
                        file_name=file_name,
                        mime=mime_type,
                        use_container_width=True
                    )
                    
                except Exception as e:
                    st.error(f"❌ Erro durante exportação: {e}")
                    import traceback
                    st.code(traceback.format_exc())
    
    else:
        st.warning("Nenhum registro encontrado com os filtros aplicados.")
    
else:
    st.warning("""
    ⚠️ **Configuração necessária:**
    
    1. Adicione seu token do Hugging Face nas secrets do Streamlit Cloud
    2. Acesse: Settings → Secrets
    3. Adicione: `HF_TOKEN = "seu_token_aqui"`
    
    Obtenha seu token em: https://huggingface.co/settings/tokens
    """)

# ==========================================
# DICAS DE PERFORMANCE
# ==========================================
with st.expander("💡 Dicas para melhor performance"):
    st.markdown("""
    **Para datasets grandes (188MB+):**
    
    1. **Use filtros específicos** - Limite os resultados antes de exportar
    2. **Prefira CSV para grandes volumes** - Mais eficiente que Excel
    3. **Exporte apenas colunas necessárias** - Marque "Exportar apenas member_pk"
    4. **Use intervalos de data** - Filtre por período específico
    
    **Limitações do Streamlit Cloud:**
    - Memória limitada (1GB no plano gratuito)
    - Processamento otimizado para lotes
    - Cache inteligente para queries repetidas
    """)
