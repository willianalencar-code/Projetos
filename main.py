import streamlit as st
import duckdb
from huggingface_hub import hf_hub_download
import pandas as pd
import tempfile
from datetime import datetime
import pyarrow.parquet as pq
import warnings
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots

warnings.filterwarnings('ignore')

# ==========================================
# CONFIGURAÇÃO DA PÁGINA - MODERNA
# ==========================================
st.set_page_config(
    page_title="🔍 Segmentação de Clientes",
    layout="wide",
    page_icon="👥",
    initial_sidebar_state="collapsed"
)

# ==========================================
# ESTILO CSS PERSONALIZADO
# ==========================================
st.markdown("""
<style>
    /* Estilo minimalista */
    .main {
        padding: 1rem 2rem;
    }
    
    /* Cabeçalhos */
    h1, h2, h3 {
        color: #1E3A8A;
        font-weight: 600;
        margin-top: 0;
    }
    
    /* Cards */
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
    
    /* Botões */
    .stButton>button {
        width: 100%;
        border-radius: 8px;
        border: none;
        font-weight: 600;
        transition: all 0.3s;
    }
    
    .stButton>button:hover {
        transform: translateY(-2px);
        box-shadow: 0 4px 12px rgba(59, 130, 246, 0.2);
    }
    
    /* Inputs */
    .stSelectbox, .stMultiselect, .stDateInput, .stTextInput {
        margin-bottom: 0.5rem;
    }
    
    /* Badges */
    .status-badge {
        display: inline-block;
        padding: 0.25rem 0.75rem;
        border-radius: 20px;
        font-size: 0.875rem;
        font-weight: 500;
    }
    
    .status-active {
        background: #D1FAE5;
        color: #065F46;
    }
    
    .status-inactive {
        background: #FEE2E2;
        color: #991B1B;
    }
    
    /* Separador */
    .divider {
        border-top: 2px solid #E2E8F0;
        margin: 2rem 0;
    }
</style>
""", unsafe_allow_html=True)

# ==========================================
# FUNÇÕES CACHE OTIMIZADAS
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
        SELECT 
            categoria,
            setor,
            data_ultima_visita,
            data_ultima_compra,
            COUNT(*) as count
        FROM read_parquet('{caminho_local}')
        GROUP BY 1,2,3,4
        LIMIT 10000
        """
        
        sample_df = con.execute(sample_query).df()
        
        # Informações básicas
        categorias = sample_df['categoria'].dropna().unique().tolist()[:20]
        setores = sample_df['setor'].dropna().unique().tolist()[:20]
        
        # Datas
        min_visita = sample_df['data_ultima_visita'].min()
        max_visita = sample_df['data_ultima_visita'].max()
        min_compra = sample_df['data_ultima_compra'].min()
        max_compra = sample_df['data_ultima_compra'].max()
        
        con.close()
        
        return {
            'caminho': caminho_local,
            'num_rows': num_rows,
            'categorias': sorted(categorias),
            'setores': sorted(setores),
            'min_visita': pd.Timestamp(min_visita) if pd.notna(min_visita) else pd.Timestamp('2020-01-01'),
            'max_visita': pd.Timestamp(max_visita) if pd.notna(max_visita) else pd.Timestamp.now(),
            'min_compra': pd.Timestamp(min_compra) if pd.notna(min_compra) else pd.Timestamp('2020-01-01'),
            'max_compra': pd.Timestamp(max_compra) if pd.notna(max_compra) else pd.Timestamp.now()
        }
        
    except Exception as e:
        st.error(f"Erro de conexão: {e}")
        return None

@st.cache_resource(show_spinner=False)
def get_connection():
    return duckdb.connect(database=':memory:')

# ==========================================
# CABEÇALHO MODERNO
# ==========================================
col1, col2 = st.columns([3, 1])
with col1:
    st.markdown("# 👥 Segmentação de Clientes")
    st.markdown("**Filtre e exporte sua base de clientes de forma inteligente**")
with col2:
    st.markdown("")
    if st.button("🔄 Atualizar Dados", type="secondary"):
        st.cache_data.clear()
        st.rerun()

# ==========================================
# CARREGAMENTO DOS DADOS
# ==========================================
with st.spinner("📦 Carregando informações..."):
    dataset_info = get_dataset_info()

if not dataset_info:
    st.error("❌ Não foi possível carregar os dados. Verifique sua conexão.")
    st.stop()

# ==========================================
# VISÃO GERAL - CARDS DE MÉTRICAS
# ==========================================
st.markdown("### 📊 Visão Geral da Base")
col1, col2, col3, col4 = st.columns(4)

with col1:
    st.markdown('<div class="metric-card">', unsafe_allow_html=True)
    st.metric("Total de Registros", f"{dataset_info['num_rows']:,}")
    st.markdown('</div>', unsafe_allow_html=True)

with col2:
    st.markdown('<div class="metric-card">', unsafe_allow_html=True)
    st.metric("Categorias", len(dataset_info['categorias']))
    st.markdown('</div>', unsafe_allow_html=True)

with col3:
    st.markdown('<div class="metric-card">', unsafe_allow_html=True)
    st.metric("Setores", len(dataset_info['setores']))
    st.markdown('</div>', unsafe_allow_html=True)

with col4:
    periodo = f"{dataset_info['min_visita'].date()} a {dataset_info['max_visita'].date()}"
    st.markdown('<div class="metric-card">', unsafe_allow_html=True)
    st.metric("Período", periodo[:20] + "..." if len(periodo) > 20 else periodo)
    st.markdown('</div>', unsafe_allow_html=True)

# ==========================================
# SEÇÃO DE FILTROS - SIDEBAR ESTILIZADA
# ==========================================
with st.sidebar:
    st.markdown("### 🔍 Filtros Avançados")
    
    # Filtros em cards
    with st.container():
        st.markdown('<div class="filter-card">', unsafe_allow_html=True)
        st.markdown("**📋 Filtros Principais**")
        
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
        st.markdown('</div>', unsafe_allow_html=True)
    
    # Filtros de Data
    with st.container():
        st.markdown('<div class="filter-card">', unsafe_allow_html=True)
        st.markdown("**📅 Filtros Temporais**")
        
        # Data de Visita
        st.markdown("##### Última Visita")
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
        
        # Data de Compra (com toggle)
        st.markdown("##### Última Compra")
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
    
    # Opções de Exportação
    with st.container():
        st.markdown('<div class="filter-card">', unsafe_allow_html=True)
        st.markdown("**⚙️ Configurações**")
        
        only_member_pk = st.checkbox("Exportar apenas IDs", value=False)
        export_format = st.radio("Formato de Saída:", 
                                ["CSV", "Excel"], 
                                horizontal=True)
        st.markdown('</div>', unsafe_allow_html=True)
    
    # Botão de Ação Principal
    if st.button("🎯 Aplicar Filtros & Analisar", type="primary", use_container_width=True):
        st.session_state.filtro_aplicado = True

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
    
    conditions.append(f"data_ultima_visita >= '{data_inicio_visita}'")
    conditions.append(f"data_ultima_visita <= '{data_fim_visita}'")
    
    if usar_compra and data_inicio_compra and data_fim_compra:
        conditions.append(f"data_ultima_compra >= '{data_inicio_compra}'")
        conditions.append(f"data_ultima_compra <= '{data_fim_compra}'")
    
    return " AND ".join(conditions) if conditions else "1=1"

# ==========================================
# ANÁLISE DOS RESULTADOS FILTRADOS
# ==========================================
if 'filtro_aplicado' in st.session_state and st.session_state.filtro_aplicado:
    where_clause = build_query_conditions()
    
    # Query para análise
    con = get_connection()
    analysis_query = f"""
    WITH filtered AS (
        SELECT * 
        FROM read_parquet('{dataset_info['caminho']}')
        WHERE {where_clause}
    )
    SELECT 
        COUNT(*) as total_registros,
        COUNT(DISTINCT member_pk) as clientes_unicos,
        AVG(CASE WHEN data_ultima_compra IS NOT NULL THEN 1 ELSE 0 END) * 100 as taxa_conversao,
        MIN(data_ultima_visita) as primeira_visita,
        MAX(data_ultima_visita) as ultima_visita
    FROM filtered
    """
    
    result = con.execute(analysis_query).fetchone()
    
    if result[0] > 0:
        total_filtrado, clientes_unicos, taxa_conversao, primeira_visita, ultima_visita = result
        
        # ==========================================
        # RESUMO DOS RESULTADOS
        # ==========================================
        st.markdown("### 📈 Resultados da Filtragem")
        
        col1, col2, col3, col4 = st.columns(4)
        
        with col1:
            st.markdown('<div class="metric-card">', unsafe_allow_html=True)
            st.metric("Registros Encontrados", f"{total_filtrado:,}")
            st.markdown('</div>', unsafe_allow_html=True)
        
        with col2:
            st.markdown('<div class="metric-card">', unsafe_allow_html=True)
            st.metric("Clientes Únicos", f"{clientes_unicos:,}")
            st.markdown('</div>', unsafe_allow_html=True)
        
        with col3:
            st.markdown('<div class="metric-card">', unsafe_allow_html=True)
            st.metric("Taxa de Conversão", f"{taxa_conversao:.1f}%")
            st.markdown('</div>', unsafe_allow_html=True)
        
        with col4:
            st.markdown('<div class="metric-card">', unsafe_allow_html=True)
            periodo_filtrado = f"{primeira_visita.date()} a {ultima_visita.date()}"
            st.metric("Período Filtrado", periodo_filtrado)
            st.markdown('</div>', unsafe_allow_html=True)
        
        # ==========================================
        # VISUALIZAÇÕES
        # ==========================================
        col_viz1, col_viz2 = st.columns(2)
        
        with col_viz1:
            # Distribuição por categoria
            cat_query = f"""
            SELECT categoria, COUNT(*) as count
            FROM read_parquet('{dataset_info['caminho']}')
            WHERE {where_clause}
            GROUP BY categoria
            ORDER BY count DESC
            LIMIT 10
            """
            
            cat_df = con.execute(cat_query).df()
            
            if not cat_df.empty:
                fig1 = px.bar(cat_df, x='categoria', y='count',
                             title="📊 Top 10 Categorias",
                             color='count',
                             color_continuous_scale='blues')
                fig1.update_layout(height=300, showlegend=False)
                st.plotly_chart(fig1, use_container_width=True)
        
        with col_viz2:
            # Evolução temporal
            time_query = f"""
            SELECT 
                DATE(data_ultima_visita) as data,
                COUNT(*) as visitas
            FROM read_parquet('{dataset_info['caminho']}')
            WHERE {where_clause}
            GROUP BY DATE(data_ultima_visita)
            ORDER BY data
            LIMIT 30
            """
            
            time_df = con.execute(time_query).df()
            
            if not time_df.empty:
                fig2 = px.line(time_df, x='data', y='visitas',
                              title="📈 Visitas (Últimos 30 dias)",
                              markers=True)
                fig2.update_layout(height=300)
                st.plotly_chart(fig2, use_container_width=True)
        
        # ==========================================
        # PRÉ-VISUALIZAÇÃO DOS DADOS
        # ==========================================
        with st.expander("👁️ **Pré-visualização dos Dados**", expanded=True):
            preview_query = f"""
            SELECT member_pk, categoria, setor, 
                   data_ultima_visita, data_ultima_compra
            FROM read_parquet('{dataset_info['caminho']}')
            WHERE {where_clause}
            LIMIT 100
            """
            
            preview_df = con.execute(preview_query).df()
            
            if not preview_df.empty:
                # Formatação das datas
                preview_df['data_ultima_visita'] = pd.to_datetime(preview_df['data_ultima_visita']).dt.strftime('%d/%m/%Y')
                preview_df['data_ultima_compra'] = pd.to_datetime(preview_df['data_ultima_compra']).dt.strftime('%d/%m/%Y')
                
                st.dataframe(
                    preview_df,
                    use_container_width=True,
                    column_config={
                        "member_pk": "ID Cliente",
                        "categoria": "Categoria",
                        "setor": "Setor",
                        "data_ultima_visita": "Última Visita",
                        "data_ultima_compra": "Última Compra"
                    },
                    hide_index=True
                )
                st.caption(f"Mostrando 100 de {total_filtrado:,} registros")
        
        # ==========================================
        # EXPORTAÇÃO
        # ==========================================
        st.markdown("### 📤 Exportação")
        
        col_exp1, col_exp2 = st.columns([3, 1])
        
        with col_exp1:
            st.info(f"**Pronto para exportar:** {total_filtrado:,} registros • {clientes_unicos:,} clientes únicos")
        
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
                            st.error(f"❌ Erro na exportação: {e}")
        
        con.close()
    
    else:
        st.warning("⚠️ Nenhum registro encontrado com os filtros aplicados.")
        st.info("Tente ajustar os critérios de filtragem.")

else:
    # ==========================================
    # ESTADO INICIAL - GUIA DE USO
    # ==========================================
    st.markdown("---")
    
    col_guide1, col_guide2, col_guide3 = st.columns(3)
    
    with col_guide1:
        st.markdown('<div class="filter-card">', unsafe_allow_html=True)
        st.markdown("### 🔍 Passo 1")
        st.markdown("Configure os filtros na **barra lateral**")
        st.markdown("• Selecione categorias")
        st.markdown("• Defina períodos")
        st.markdown("• Escolha setores")
        st.markdown('</div>', unsafe_allow_html=True)
    
    with col_guide2:
        st.markdown('<div class="filter-card">', unsafe_allow_html=True)
        st.markdown("### 📊 Passo 2")
        st.markdown("Clique em **'Aplicar Filtros & Analisar'**")
        st.markdown("• Visualize resultados")
        st.markdown("• Analise métricas")
        st.markdown("• Verifique prévia dos dados")
        st.markdown('</div>', unsafe_allow_html=True)
    
    with col_guide3:
        st.markdown('<div class="filter-card">', unsafe_allow_html=True)
        st.markdown("### 📤 Passo 3")
        st.markdown("Exporte os dados filtrados")
        st.markdown("• Escolha formato (CSV/Excel)")
        st.markdown("• Baixe o arquivo")
        st.markdown("• Use em suas análises")
        st.markdown('</div>', unsafe_allow_html=True)

# ==========================================
# RODAPÉ
# ==========================================
st.markdown("---")
col_foot1, col_foot2 = st.columns([3, 1])
with col_foot1:
    st.caption(f"📊 Base de dados: {dataset_info['num_rows']:,} registros • Última atualização: {datetime.now().strftime('%d/%m/%Y %H:%M')}")
with col_foot2:
    st.caption("⚡ Desenvolvido para análise de segmentação")
