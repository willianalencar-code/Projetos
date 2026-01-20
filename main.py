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
        
        # Para obter informações usando DuckDB para eficiência
        con = duckdb.connect(database=':memory:')
        
        # Obtém categorias únicas
        try:
            categorias_df = con.execute(f"""
                SELECT DISTINCT categoria 
                FROM read_parquet('{caminho_local}') 
                WHERE categoria IS NOT NULL 
                LIMIT 1000
            """).df()
            categorias = categorias_df['categoria'].tolist()
        except:
            categorias = []
        
        # Obtém setores únicos
        try:
            setores_df = con.execute(f"""
                SELECT DISTINCT setor 
                FROM read_parquet('{caminho_local}') 
                WHERE setor IS NOT NULL 
                LIMIT 1000
            """).df()
            setores = setores_df['setor'].tolist()
        except:
            setores = []
        
        # Obtém datas mínimas e máximas
        try:
            dates_df = con.execute(f"""
                SELECT 
                    MIN(data_ultima_visita) as min_visita,
                    MAX(data_ultima_visita) as max_visita,
                    MIN(data_ultima_compra) as min_compra,
                    MAX(data_ultima_compra) as max_compra
                FROM read_parquet('{caminho_local}')
            """).df()
            
            min_visita = dates_df['min_visita'].iloc[0]
            max_visita = dates_df['max_visita'].iloc[0]
            min_compra = dates_df['min_compra'].iloc[0]
            max_compra = dates_df['max_compra'].iloc[0]
        except:
            min_visita = pd.Timestamp('2020-01-01')
            max_visita = pd.Timestamp.now()
            min_compra = pd.Timestamp('2020-01-01')
            max_compra = pd.Timestamp.now()
        
        # Obtém member_pk únicos
        try:
            unique_members = con.execute(f"""
                SELECT COUNT(DISTINCT member_pk) as unique_members
                FROM read_parquet('{caminho_local}')
            """).fetchone()[0]
        except:
            unique_members = num_rows  # Fallback
        
        con.close()
        
        return {
            'caminho': caminho_local,
            'num_rows': num_rows,
            'unique_members': unique_members,
            'categorias': sorted(categorias),
            'setores': sorted(setores),
            'min_visita': min_visita,
            'max_visita': max_visita,
            'min_compra': min_compra,
            'max_compra': max_compra
        }
    except Exception as e:
        st.error(f"Erro de conexão: {e}")
        import traceback
        st.code(traceback.format_exc())
        return None

@st.cache_resource(show_spinner=False)
def get_connection():
    return duckdb.connect(database=':memory:')

# ==========================================
# INICIALIZAÇÃO OTIMIZADA
# ==========================================
st.title("📋 Gestão e Exportação de Clientes")

# Carrega apenas informações do dataset
with st.spinner("Carregando informações do dataset..."):
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
    
    # Filtros de data
    st.sidebar.subheader("📅 Filtros por Data")
    
    # Última visita
    col1, col2 = st.sidebar.columns(2)
    
    with col1:
        data_inicio_visita = st.date_input(
            "Data início visita",
            value=dataset_info['min_visita'].date(),
            key="data_inicio_visita"
        )
    
    with col2:
        data_fim_visita = st.date_input(
            "Data fim visita",
            value=dataset_info['max_visita'].date(),
            key="data_fim_visita"
        )
    
    # Última compra - COM CHECKBOX PARA ATIVAR/DESATIVAR
    st.sidebar.markdown("---")
    usar_filtro_compra = st.sidebar.checkbox(
        "Usar filtro de data da última compra", 
        value=False,
        help="Marque esta opção para filtrar por data da última compra. Desmarque para ignorar este filtro."
    )
    
    if usar_filtro_compra:
        col3, col4 = st.sidebar.columns(2)
        
        with col3:
            data_inicio_compra = st.date_input(
                "Data início compra",
                value=dataset_info['min_compra'].date(),
                key="data_inicio_compra"
            )
        
        with col4:
            data_fim_compra = st.date_input(
                "Data fim compra",
                value=dataset_info['max_compra'].date(),
                key="data_fim_compra"
            )
    else:
        # Define valores padrão quando o filtro está desativado
        data_inicio_compra = None
        data_fim_compra = None
    
    # opção somente member_pk
    only_member_pk = st.sidebar.checkbox("Exportar apenas member_pk", value=False)
    
    # ==========================================
    # CONSTRUÇÃO DA QUERY COM PROCESSAMENTO EM LOTES
    # ==========================================
    def build_query_conditions():
        conditions = []
        
        if id_busca and id_busca.strip():
            try:
                # Tenta converter para inteiro se possível
                int(id_busca)
                conditions.append(f"member_pk = {id_busca}")
            except:
                conditions.append(f"CAST(member_pk AS VARCHAR) LIKE '%{id_busca}%'")
        
        if cat_sel:
            cat_list = ', '.join([f"'{c}'" for c in cat_sel])
            conditions.append(f"categoria IN ({cat_list})")
        
        if setor_sel:
            setor_list = ', '.join([f"'{s}'" for s in setor_sel])
            conditions.append(f"setor IN ({setor_list})")
        
        # Filtro de data da última visita (sempre aplicado)
        conditions.append(f"data_ultima_visita >= '{data_inicio_visita}'")
        conditions.append(f"data_ultima_visita <= '{data_fim_visita}'")
        
        # Filtro de data da última compra (só se ativado)
        if usar_filtro_compra and data_inicio_compra and data_fim_compra:
            conditions.append(f"data_ultima_compra >= '{data_inicio_compra}'")
            conditions.append(f"data_ultima_compra <= '{data_fim_compra}'")
        
        return conditions
    
    # ==========================================
    # ESTIMATIVA DE RESULTADOS
    # ==========================================
    st.sidebar.header("📊 Estatísticas")
    
    # Botão para estimar resultados
    if st.sidebar.button("🔄 Calcular Estimativa", key="calc_estimate"):
        with st.spinner("Calculando estimativa..."):
            conditions = build_query_conditions()
            where_clause = " AND ".join(conditions) if conditions else "1=1"
            
            # Query para contar resultados
            count_query = f"""
            SELECT 
                COUNT(*) as total_registros,
                COUNT(DISTINCT member_pk) as unique_members
            FROM read_parquet('{dataset_info['caminho']}')
            WHERE {where_clause}
            """
            
            try:
                con = get_connection()
                result = con.execute(count_query).fetchone()
                total_filtrado = result[0]
                unique_members_filtrado = result[1]
                
                st.session_state.total_filtrado = total_filtrado
                st.session_state.unique_members_filtrado = unique_members_filtrado
                st.session_state.where_clause = where_clause
                
                st.sidebar.success(f"Estimativa: {total_filtrado:,} registros")
                st.sidebar.success(f"Clientes únicos: {unique_members_filtrado:,}")
                
            except Exception as e:
                st.sidebar.error(f"Erro na contagem: {e}")
                st.session_state.total_filtrado = 0
                st.session_state.unique_members_filtrado = 0
    
    # Mostra estimativa se existir
    if 'total_filtrado' in st.session_state:
        total_filtrado = st.session_state.total_filtrado
        unique_members_filtrado = st.session_state.unique_members_filtrado
        where_clause = st.session_state.where_clause
        
        # Ajusta formato de exportação baseado no tamanho
        if total_filtrado > 100000:
            export_format = "CSV"
            st.sidebar.warning(f"⚠️ {total_filtrado:,} registros - CSV recomendado")
        else:
            export_format = st.sidebar.selectbox(
                "Formato de exportação:",
                ["CSV", "Excel (.xlsx)"],
                key="export_format"
            )
        
        st.sidebar.metric("Registros filtrados", f"{total_filtrado:,}")
        st.sidebar.metric("Clientes únicos", f"{unique_members_filtrado:,}")
    else:
        total_filtrado = 0
        unique_members_filtrado = 0
        export_format = "CSV"
        where_clause = "1=1"
    
    # ==========================================
    # MÉTRICAS PRINCIPAIS - APENAS TAMANHO DA BASE E MEMBER_PK ÚNICOS
    # ==========================================
    col1, col2, col3 = st.columns(3)
    
    # Tamanho total da base
    col1.metric("📊 Tamanho total da base", f"{dataset_info['num_rows']:,}")
    
    # Member_pk únicos na base completa
    col2.metric("👥 Clientes únicos totais", f"{dataset_info['unique_members']:,}")
    
    # Status do filtro de compra
    if usar_filtro_compra:
        status_filtro = "✅ Ativo"
    else:
        status_filtro = "❌ Inativo"
    
    col3.metric("🎯 Filtro de compra", status_filtro)
    
    # ==========================================
    # PRÉ-VISUALIZAÇÃO OTIMIZADA
    # ==========================================
    st.subheader("📋 Pré-visualização (50 linhas)")
    
    # Query para preview otimizada
    select_cols = "member_pk" if only_member_pk else "*"
    
    if 'where_clause' in st.session_state:
        preview_query = f"""
        SELECT {select_cols}
        FROM read_parquet('{dataset_info['caminho']}')
        WHERE {where_clause}
        LIMIT 50
        """
        
        try:
            con = get_connection()
            df_preview = con.execute(preview_query).df()
            
            if not df_preview.empty:
                # Formata colunas de data
                date_cols = ['data_ultima_visita', 'data_ultima_compra']
                for col in date_cols:
                    if col in df_preview.columns:
                        # Converte para datetime
                        df_preview[col] = pd.to_datetime(df_preview[col], errors='coerce')
                
                # Mostra dataframe
                st.dataframe(
                    df_preview,
                    use_container_width=True,
                    column_config={
                        "data_ultima_compra": st.column_config.DatetimeColumn(
                            "Última compra",
                            format="DD/MM/YYYY",
                        ),
                        "data_ultima_visita": st.column_config.DatetimeColumn(
                            "Última visita",
                            format="DD/MM/YYYY",
                        ),
                    }
                )
                st.caption(f"Mostrando 50 de {total_filtrado:,} registros ({unique_members_filtrado:,} clientes únicos)")
                
            else:
                st.info("Nenhum resultado encontrado com os filtros atuais.")
                
        except Exception as e:
            st.error(f"Erro na pré-visualização: {e}")
    else:
        st.info("Configure os filtros e clique em 'Calcular Estimativa' para ver a pré-visualização.")
    
    # ==========================================
    # EXPORTAÇÃO CORRIGIDA
    # ==========================================
    st.header("📤 Exportação")
    
    if 'total_filtrado' in st.session_state and st.session_state.total_filtrado > 0:
        total_filtrado = st.session_state.total_filtrado
        unique_members_filtrado = st.session_state.unique_members_filtrado
        
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        
        # Determina extensão baseada no formato
        if export_format == "Excel (.xlsx)":
            file_ext = "xlsx"
            mime_type = "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
        else:
            file_ext = "csv"
            mime_type = "text/csv"
        
        file_name = f"clientes_{timestamp}.{file_ext}"
        
        st.info(f"📄 Nome do arquivo: **{file_name}**")
        st.info(f"📊 Total de registros: **{total_filtrado:,}**")
        st.info(f"👥 Clientes únicos: **{unique_members_filtrado:,}**")
        
        # Informações sobre o filtro aplicado
        if usar_filtro_compra:
            st.info(f"🎯 **Filtro compra:** {data_inicio_compra} a {data_fim_compra}")
        else:
            st.info("🎯 **Filtro compra:** Desativado")
        
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
                    
                    # Conecta ao DuckDB
                    con = get_connection()
                    
                    # Query para exportação
                    select_cols = "member_pk" if only_member_pk else "*"
                    export_query = f"""
                    SELECT {select_cols}
                    FROM read_parquet('{dataset_info['caminho']}')
                    WHERE {where_clause}
                    """
                    
                    status_text.text(f"Iniciando exportação de {total_filtrado:,} registros...")
                    
                    if export_format == "Excel (.xlsx)":
                        # Para Excel, processamos tudo de uma vez (apenas para volumes menores)
                        status_text.text("Exportando para Excel...")
                        
                        df_export = con.execute(export_query).df()
                        
                        # Verifica se o dataframe não está vazio
                        if not df_export.empty:
                            # Exporta para Excel
                            with pd.ExcelWriter(tmp_path, engine='openpyxl') as writer:
                                df_export.to_excel(writer, index=False, sheet_name='Clientes')
                            
                            progress_bar.progress(1.0)
                        else:
                            # CORREÇÃO: Usar st.stop() em vez de return
                            st.error("Nenhum dado para exportar!")
                            progress_bar.empty()
                            status_text.empty()
                            st.stop()
                    
                    else:
                        # Para CSV, processamos em lotes
                        status_text.text("Exportando em lotes (CSV)...")
                        
                        # Configura lote
                        batch_size = 100000
                        first_batch = True
                        processed_rows = 0
                        
                        # Exporta em lotes usando iterator
                        result = con.execute(export_query)
                        
                        batch_count = 0
                        while True:
                            batch = result.fetch_df_chunk(batch_size)
                            if batch.empty:
                                break
                            
                            batch_count += 1
                            
                            if first_batch:
                                batch.to_csv(tmp_path, index=False)
                                first_batch = False
                            else:
                                batch.to_csv(tmp_path, mode='a', header=False, index=False)
                            
                            processed_rows += len(batch)
                            progress = min(processed_rows / total_filtrado, 1.0)
                            progress_bar.progress(progress)
                            status_text.text(f"Lote {batch_count}: {processed_rows:,} de {total_filtrado:,} registros")
                    
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
                    st.download_button(
                        label=f"📥 BAIXAR {file_ext.upper()} ({file_size:.2f} MB)",
                        data=file_data,
                        file_name=file_name,
                        mime=mime_type,
                        use_container_width=True
                    )
                    
                except Exception as e:
                    st.error(f"❌ Erro durante exportação: {e}")
                    import traceback
                    st.code(traceback.format_exc())
    
    elif 'total_filtrado' in st.session_state and st.session_state.total_filtrado == 0:
        st.warning("Nenhum registro encontrado com os filtros aplicados.")
    else:
        st.info("Configure os filtros e clique em 'Calcular Estimativa' para habilitar a exportação.")
    
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
    3. **Exporte apenas member_pk** - Marque "Exportar apenas member_pk" para arquivos menores
    4. **Use intervalos de data** - Filtre por período específico
    5. **Calcule estimativa primeiro** - Evite exportar dados indesejados
    
    **Formato de exportação:**
    - ✅ **CSV**: Recomendado para mais de 100.000 registros
    - ✅ **Excel**: Adequado para até 100.000 registros
    
    **Filtro de última compra:**
    - ✅ **Desmarque a checkbox** para ignorar completamente o filtro de compra
    - ✅ **Marque a checkbox** para ativar o filtro por data de compra
    
    **Limitações do Streamlit Cloud:**
    - Memória limitada (1GB no plano gratuito)
    - Processamento otimizado para lotes
    - Cache inteligente para queries repetidas
    """)

# ==========================================
# INFORMAÇÕES DO SISTEMA
# ==========================================
with st.expander("ℹ️ Informações do Sistema"):
    if dataset_info:
        st.write(f"**Dataset carregado:** {os.path.basename(dataset_info['caminho'])}")
        st.write(f"**Tamanho total:** {dataset_info['num_rows']:,} registros")
        st.write(f"**Clientes únicos:** {dataset_info['unique_members']:,} member_pk distintos")
        st.write(f"**Categorias disponíveis:** {len(dataset_info['categorias'])}")
        st.write(f"**Setores disponíveis:** {len(dataset_info['setores'])}")
        st.write(f"**Período de visitas:** {dataset_info['min_visita'].date()} a {dataset_info['max_visita'].date()}")
        st.write(f"**Período de compras:** {dataset_info['min_compra'].date()} a {dataset_info['max_compra'].date()}")
