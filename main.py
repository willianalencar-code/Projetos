import streamlit as st
import duckdb
from huggingface_hub import hf_hub_download
import pandas as pd
import tempfile
from datetime import datetime
import pyarrow.parquet as pq
import warnings

warnings.filterwarnings('ignore')

# ==========================================
# CONFIGURAÇÃO DA PÁGINA
# ==========================================
st.set_page_config(
    page_title="🔍 Segmentação de Clientes",
    layout="wide",
    page_icon="👥",
    initial_sidebar_state="expanded"
)

# ==========================================
# ESTILO CSS
# ==========================================
st.markdown("""
<style>
    .metric-card {
        background: white;
        padding: 1.5rem;
        border-radius: 12px;
        border-left: 4px solid #3B82F6;
        box-shadow: 0 2px 8px rgba(0,0,0,0.05);
        margin-bottom: 1rem;
    }
    
    .filter-card {
        background: #F8FAFC;
        padding: 1.5rem;
        border-radius: 12px;
        border: 1px solid #E2E8F0;
        margin-bottom: 1rem;
    }
</style>
""", unsafe_allow_html=True)

# ==========================================
# FUNÇÕES CACHE
# ==========================================
@st.cache_data(show_spinner=False, ttl=3600)
def get_dataset_info():
    """Obtém informações do dataset de forma eficiente"""
    try:
        token = st.secrets.get("HF_TOKEN", "")
        caminho_local = hf_hub_download(
            repo_id="WillianAlencar/SegmentacaoClientes",
            filename="dataset.parquet",
            repo_type="dataset",
            token=token if token else None
        )
        
        # Contagem via metadados
        parquet_file = pq.ParquetFile(caminho_local)
        num_rows = parquet_file.metadata.num_rows
        
        # Carrega amostra para análise
        con = duckdb.connect(database=':memory:')
        
        # Amostra para análise
        sample_query = f"""
        SELECT DISTINCT categoria, setor
        FROM read_parquet('{caminho_local}')
        WHERE categoria IS NOT NULL AND setor IS NOT NULL
        LIMIT 100
        """
        
        sample_df = con.execute(sample_query).df()
        
        # Informações básicas
        categorias = sample_df['categoria'].dropna().unique().tolist()
        setores = sample_df['setor'].dropna().unique().tolist()
        
        # Datas min/max para todos os campos de data
        dates_query = f"""
        SELECT 
            MIN(data_ultima_visita) as min_visita,
            MAX(data_ultima_visita) as max_visita,
            MIN(data_ultima_compra) as min_compra,
            MAX(data_ultima_compra) as max_compra,
            MIN(data_cadastro) as min_cadastro,
            MAX(data_cadastro) as max_cadastro
        FROM read_parquet('{caminho_local}')
        """
        
        dates_df = con.execute(dates_query).df()
        
        # Verifica se campos premium existem
        try:
            columns_query = f"""
            SELECT column_name 
            FROM parquet_schema('{caminho_local}')
            """
            columns_df = con.execute(columns_query).df()
            has_flg_premium = 'flg_premium_ativo' in columns_df['column_name'].values
        except:
            has_flg_premium = False
        
        con.close()
        
        return {
            'caminho': caminho_local,
            'num_rows': num_rows,
            'categorias': sorted(categorias),
            'setores': sorted(setores),
            'min_visita': dates_df['min_visita'].iloc[0] if not dates_df.empty else pd.Timestamp('2020-01-01'),
            'max_visita': dates_df['max_visita'].iloc[0] if not dates_df.empty else pd.Timestamp.now(),
            'min_compra': dates_df['min_compra'].iloc[0] if not dates_df.empty else pd.Timestamp('2020-01-01'),
            'max_compra': dates_df['max_compra'].iloc[0] if not dates_df.empty else pd.Timestamp.now(),
            'min_cadastro': dates_df['min_cadastro'].iloc[0] if not dates_df.empty else pd.Timestamp('2020-01-01'),
            'max_cadastro': dates_df['max_cadastro'].iloc[0] if not dates_df.empty else pd.Timestamp.now(),
            'has_flg_premium': has_flg_premium
        }
        
    except Exception as e:
        st.error(f"Erro de conexão: {e}")
        return None

# ==========================================
# CABEÇALHO
# ==========================================
st.markdown("# 👥 Segmentação de Clientes")
st.markdown("**Filtre e exporte sua base de clientes**")

# ==========================================
# CARREGAMENTO DOS DADOS
# ==========================================
with st.spinner("📦 Carregando informações..."):
    dataset_info = get_dataset_info()

if not dataset_info:
    st.error("❌ Não foi possível carregar os dados. Verifique sua conexão.")
    st.stop()

# ==========================================
# SEÇÃO DE FILTROS - SIDEBAR
# ==========================================
with st.sidebar:
    st.markdown("### 🔍 Filtros Avançados")
    
    with st.container():
        st.markdown('<div class="filter-card">', unsafe_allow_html=True)
        st.markdown("**Filtros Principais**")
        
        # Busca por ID
        id_busca = st.text_input("Buscar Cliente (ID)", 
                                 placeholder="Digite o member_pk...")
        
        # Categorias
        cat_sel = st.multiselect("Categorias", 
                                dataset_info['categorias'],
                                placeholder="Selecione categorias...")
        
        # Setores
        setor_sel = st.multiselect("Setores", 
                                  dataset_info['setores'],
                                  placeholder="Selecione setores...")
        
        # Filtro para clientes sem compra
        apenas_sem_compra = st.checkbox("Apenas clientes sem compra", value=False)
        
        # Filtro para retirar premium
        if dataset_info['has_flg_premium']:
            excluir_premium = st.checkbox("Excluir clientes premium", value=False)
        else:
            excluir_premium = False
            st.caption("ℹ️ Campo 'flg_premium_ativo' não disponível no dataset")
            
        st.markdown('</div>', unsafe_allow_html=True)
    
    with st.container():
        st.markdown('<div class="filter-card">', unsafe_allow_html=True)
        st.markdown("**Filtros de Data**")
        
        # Data de Cadastro
        st.markdown("**Data de Cadastro**")
        usar_cadastro = st.toggle("Ativar filtro de cadastro", value=False, key="toggle_cadastro")
        
        if usar_cadastro:
            col_cad1, col_cad2 = st.columns(2)
            with col_cad1:
                data_inicio_cadastro = st.date_input("De", 
                                                    value=dataset_info['min_cadastro'].date(),
                                                    key="inicio_cadastro",
                                                    label_visibility="collapsed")
            with col_cad2:
                data_fim_cadastro = st.date_input("Até", 
                                                 value=dataset_info['max_cadastro'].date(),
                                                 key="fim_cadastro",
                                                 label_visibility="collapsed")
        else:
            data_inicio_cadastro = None
            data_fim_cadastro = None
        
        # Data de Visita
        st.markdown("**Última Visita**")
        col1, col2 = st.columns(2)
        with col1:
            data_inicio_visita = st.date_input("De", 
                                              value=dataset_info['min_visita'].date(),
                                              key="inicio_visita",
                                              label_visibility="collapsed")
        with col2:
            data_fim_visita = st.date_input("Até", 
                                           value=dataset_info['max_visita'].date(),
                                           key="fim_visita",
                                           label_visibility="collapsed")
        
        # Data de Compra
        st.markdown("**Última Compra**")
        usar_compra = st.toggle("Ativar filtro", value=False, key="toggle_compra")
        
        if usar_compra:
            col3, col4 = st.columns(2)
            with col3:
                data_inicio_compra = st.date_input("De", 
                                                  value=dataset_info['min_compra'].date(),
                                                  key="inicio_compra",
                                                  label_visibility="collapsed")
            with col4:
                data_fim_compra = st.date_input("Até", 
                                               value=dataset_info['max_compra'].date(),
                                               key="fim_compra",
                                               label_visibility="collapsed")
        else:
            data_inicio_compra = None
            data_fim_compra = None
            
        st.markdown('</div>', unsafe_allow_html=True)
    
    with st.container():
        st.markdown('<div class="filter-card">', unsafe_allow_html=True)
        st.markdown("**Configurações de Exportação**")
        
        only_member_pk = st.checkbox("Exportar apenas IDs", value=False)
        export_format = st.radio("Formato:", 
                                ["CSV", "Excel"], 
                                horizontal=True)
        st.markdown('</div>', unsafe_allow_html=True)

# ==========================================
# FUNÇÕES DE PROCESSAMENTO
# ==========================================
def build_query_conditions():
    conditions = []
    
    if id_busca and id_busca.strip():
        conditions.append(f"member_pk = '{id_busca}'")
    
    if cat_sel:
        cat_list = ', '.join([f"'{c}'" for c in cat_sel])
        conditions.append(f"categoria IN ({cat_list})")
    
    if setor_sel:
        setor_list = ', '.join([f"'{s}'" for s in setor_sel])
        conditions.append(f"setor IN ({setor_list})")
    
    # Filtro de data de visita
    conditions.append(f"data_ultima_visita >= '{data_inicio_visita}'")
    conditions.append(f"data_ultima_visita <= '{data_fim_visita}'")
    
    # Filtro de data de compra
    if usar_compra and data_inicio_compra and data_fim_compra:
        conditions.append(f"data_ultima_compra >= '{data_inicio_compra}'")
        conditions.append(f"data_ultima_compra <= '{data_fim_compra}'")
    
    # Filtro de data de cadastro
    if usar_cadastro and data_inicio_cadastro and data_fim_cadastro:
        conditions.append(f"data_cadastro >= '{data_inicio_cadastro}'")
        conditions.append(f"data_cadastro <= '{data_fim_cadastro}'")
    
    # Filtro para clientes sem compra
    if apenas_sem_compra:
        conditions.append("data_ultima_compra IS NULL")
    
    # Filtro para excluir premium
    if excluir_premium and dataset_info['has_flg_premium']:
        conditions.append("(flg_premium_ativo = 'N' OR flg_premium_ativo IS NULL)")
    
    return " AND ".join(conditions) if conditions else "1=1"

# ==========================================
# ANÁLISE AUTOMÁTICA (SEMPRE EXECUTA)
# ==========================================
try:
    where_clause = build_query_conditions()
    
    # Cria nova conexão DuckDB
    con = duckdb.connect(database=':memory:')
    
    # Query com mais métricas
    analysis_query = f"""
    WITH filtered AS (
        SELECT * 
        FROM read_parquet('{dataset_info['caminho']}')
        WHERE {where_clause}
    )
    SELECT 
        COUNT(*) as total_registros,
        COUNT(DISTINCT member_pk) as clientes_unicos,
        MIN(data_ultima_visita) as primeira_visita,
        MAX(data_ultima_visita) as ultima_visita,
        COUNT(CASE WHEN data_ultima_compra IS NOT NULL THEN 1 END) as com_compra,
        COUNT(CASE WHEN data_ultima_compra IS NULL THEN 1 END) as sem_compra,
        {f"COUNT(CASE WHEN flg_premium_ativo = 'S' THEN 1 END) as premium," if dataset_info['has_flg_premium'] else "0 as premium,"}
        {f"COUNT(CASE WHEN flg_premium_ativo = 'N' THEN 1 END) as nao_premium" if dataset_info['has_flg_premium'] else "0 as nao_premium"}
    FROM filtered
    """
    
    result = con.execute(analysis_query).fetchone()
    
    if result:
        if dataset_info['has_flg_premium']:
            total_filtrado, clientes_unicos, primeira_visita, ultima_visita, com_compra, sem_compra, premium, nao_premium = result
        else:
            total_filtrado, clientes_unicos, primeira_visita, ultima_visita, com_compra, sem_compra, _, _ = result
            premium, nao_premium = 0, 0
    else:
        total_filtrado, clientes_unicos, primeira_visita, ultima_visita, com_compra, sem_compra, premium, nao_premium = 0, 0, None, None, 0, 0, 0, 0
        
except Exception as e:
    st.error(f"❌ Erro na análise dos dados: {str(e)}")
    # Valores padrão em caso de erro
    total_filtrado, clientes_unicos, primeira_visita, ultima_visita, com_compra, sem_compra, premium, nao_premium = 0, 0, None, None, 0, 0, 0, 0
    con = None

# ==========================================
# RESUMO DOS RESULTADOS (3 COLUNAS APENAS)
# ==========================================
st.markdown("### 📊 Resultados")

col1, col2, col3 = st.columns(3)

with col1:
    st.markdown('<div class="metric-card">', unsafe_allow_html=True)
    st.metric("Total Registros", f"{total_filtrado:,}")
    if total_filtrado > 0:
        st.caption(f"Clientes únicos: {clientes_unicos:,}")
    st.markdown('</div>', unsafe_allow_html=True)

with col2:
    st.markdown('<div class="metric-card">', unsafe_allow_html=True)
    if total_filtrado > 0:
        perc_com_compra = (com_compra / total_filtrado * 100) if total_filtrado > 0 else 0
        st.metric("Com Compra", f"{com_compra:,}", f"{perc_com_compra:.1f}%")
        st.caption(f"Sem compra: {sem_compra:,}")
    else:
        st.metric("Com Compra", "0", "0%")
    st.markdown('</div>', unsafe_allow_html=True)

with col3:
    st.markdown('<div class="metric-card">', unsafe_allow_html=True)
    if total_filtrado > 0 and dataset_info['has_flg_premium']:
        perc_premium = (premium / total_filtrado * 100) if total_filtrado > 0 else 0
        st.metric("Clientes Premium", f"{premium:,}", f"{perc_premium:.1f}%")
        st.caption(f"Não premium: {nao_premium:,}")
    else:
        st.metric("Clientes Premium", "-", "N/D")
        if not dataset_info['has_flg_premium']:
            st.caption("Campo não disponível")
    st.markdown('</div>', unsafe_allow_html=True)

# ==========================================
# PRÉ-VISUALIZAÇÃO DOS DADOS
# ==========================================
if total_filtrado > 0 and con is not None:
    with st.expander("👁️ **Pré-visualização dos Dados**", expanded=True):
        try:
            # Define colunas para exibição
            base_cols = ['member_pk', 'categoria', 'setor', 'data_ultima_visita', 'data_ultima_compra']
            extra_cols = []
            
            # Adiciona data_cadastro se disponível
            columns_query = f"""
            SELECT column_name 
            FROM parquet_schema('{dataset_info['caminho']}')
            """
            columns_df = con.execute(columns_query).df()
            available_columns = columns_df['column_name'].values
            
            if 'data_cadastro' in available_columns:
                base_cols.append('data_cadastro')
            
            if 'flg_premium_ativo' in available_columns:
                base_cols.append('flg_premium_ativo')
            
            if 'flg_funcionario' in available_columns:
                base_cols.append('flg_funcionario')
            
            # Prepara query de preview
            cols_str = ', '.join(base_cols)
            preview_query = f"""
            SELECT {cols_str}
            FROM read_parquet('{dataset_info['caminho']}')
            WHERE {where_clause}
            LIMIT 100
            """
            
            preview_df = con.execute(preview_query).df()
            
            if not preview_df.empty:
                # Configurações das colunas para exibição
                column_config = {
                    "member_pk": "ID Cliente",
                    "categoria": "Categoria",
                    "setor": "Setor",
                    "data_ultima_visita": st.column_config.DatetimeColumn("Última Visita", format="DD/MM/YYYY"),
                    "data_ultima_compra": st.column_config.DatetimeColumn("Última Compra", format="DD/MM/YYYY"),
                }
                
                # Adiciona configuração para data_cadastro se existir
                if 'data_cadastro' in preview_df.columns:
                    column_config["data_cadastro"] = st.column_config.DatetimeColumn("Data Cadastro", format="DD/MM/YYYY")
                
                if 'flg_premium_ativo' in preview_df.columns:
                    column_config["flg_premium_ativo"] = "Premium"
                
                if 'flg_funcionario' in preview_df.columns:
                    column_config["flg_funcionario"] = "Funcionário"
                
                # Formatação das datas
                date_cols = [col for col in preview_df.columns if 'data_' in col]
                for col in date_cols:
                    preview_df[col] = pd.to_datetime(preview_df[col], errors='coerce')
                
                st.dataframe(
                    preview_df,
                    use_container_width=True,
                    column_config=column_config,
                    hide_index=True
                )
                st.caption(f"Mostrando 100 de {total_filtrado:,} registros")
            else:
                st.info("Nenhum dado para pré-visualizar")
                
        except Exception as e:
            st.error(f"Erro na pré-visualização: {str(e)}")
    
    # ==========================================
    # EXPORTAÇÃO
    # ==========================================
    st.markdown("### 📤 Exportação")
    
    col_exp1, col_exp2 = st.columns([3, 1])
    
    with col_exp1:
        # Resumo da exportação
        export_summary = f"**Pronto para exportar:** {total_filtrado:,} registros • {clientes_unicos:,} clientes únicos"
        if apenas_sem_compra:
            export_summary += " • Apenas sem compra"
        if excluir_premium:
            export_summary += " • Sem premium"
        if usar_cadastro:
            export_summary += f" • Cadastro: {data_inicio_cadastro} a {data_fim_cadastro}"
        
        st.info(export_summary)
    
    with col_exp2:
        export_disabled = total_filtrado > 1000000 and export_format == "Excel"
        
        if export_disabled:
            st.warning("Excel limitado a 1M registros")
        else:
            if st.button("🚀 Gerar Arquivo", type="primary", use_container_width=True):
                with st.spinner("Preparando exportação..."):
                    try:
                        # Query completa
                        select_cols = "member_pk" if only_member_pk else "*"
                        export_query = f"""
                        SELECT {select_cols}
                        FROM read_parquet('{dataset_info['caminho']}')
                        WHERE {where_clause}
                        """
                        
                        export_df = con.execute(export_query).df()
                        
                        # Gera arquivo
                        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
                        
                        if export_format == "Excel":
                            import io
                            buffer = io.BytesIO()
                            with pd.ExcelWriter(buffer, engine='openpyxl') as writer:
                                export_df.to_excel(writer, index=False, sheet_name='Clientes')
                            buffer.seek(0)
                            file_data = buffer.getvalue()
                            file_name = f"clientes_{timestamp}.xlsx"
                            mime_type = "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
                        else:
                            file_data = export_df.to_csv(index=False).encode('utf-8')
                            file_name = f"clientes_{timestamp}.csv"
                            mime_type = "text/csv"
                        
                        # Botão de download
                        st.download_button(
                            label=f"📥 Baixar {export_format} ({len(export_df):,} registros)",
                            data=file_data,
                            file_name=file_name,
                            mime=mime_type,
                            use_container_width=True
                        )
                        
                        st.success("✅ Arquivo gerado com sucesso!")
                        
                    except Exception as e:
                        st.error(f"❌ Erro na exportação: {str(e)}")
    
    # Fecha a conexão
    if con:
        con.close()

elif con is not None:
    st.warning("⚠️ Nenhum registro encontrado com os filtros aplicados.")
    st.info("Tente ajustar os critérios de filtragem.")
    
    if con:
        con.close()

# ==========================================
# RODAPÉ
# ==========================================
st.markdown("---")
st.caption(f"📊 Base de dados: {dataset_info['num_rows']:,} registros • Última atualização: {datetime.now().strftime('%d/%m/%Y %H:%M')}")
