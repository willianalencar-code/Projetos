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
        
        # Datas min/max
        dates_query = f"""
        SELECT 
            MIN(data_ultima_visita) as min_visita,
            MAX(data_ultima_visita) as max_visita,
            MIN(data_ultima_compra) as min_compra,
            MAX(data_ultima_compra) as max_compra
        FROM read_parquet('{caminho_local}')
        """
        
        dates_df = con.execute(dates_query).df()
        
        con.close()
        
        return {
            'caminho': caminho_local,
            'num_rows': num_rows,
            'categorias': sorted(categorias),
            'setores': sorted(setores),
            'min_visita': dates_df['min_visita'].iloc[0] if not dates_df.empty else pd.Timestamp('2020-01-01'),
            'max_visita': dates_df['max_visita'].iloc[0] if not dates_df.empty else pd.Timestamp.now(),
            'min_compra': dates_df['min_compra'].iloc[0] if not dates_df.empty else pd.Timestamp('2020-01-01'),
            'max_compra': dates_df['max_compra'].iloc[0] if not dates_df.empty else pd.Timestamp.now()
        }
        
    except Exception as e:
        st.error(f"Erro de conexão: {e}")
        return None

@st.cache_resource(show_spinner=False)
def get_connection():
    return duckdb.connect(database=':memory:')

# ==========================================
# CABEÇALHO
# ==========================================
st.markdown("# 👥 Segmentação de Clientes")
st.markdown("**Filtre e exporte sua base de clientes de forma inteligente**")

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
        
        # Data de Compra
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
    
    with st.container():
        st.markdown('<div class="filter-card">', unsafe_allow_html=True)
        st.markdown("**⚙️ Configurações**")
        
        only_member_pk = st.checkbox("Exportar apenas IDs", value=False)
        export_format = st.radio("Formato de Saída:", 
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
    
    conditions.append(f"data_ultima_visita >= '{data_inicio_visita}'")
    conditions.append(f"data_ultima_visita <= '{data_fim_visita}'")
    
    if usar_compra and data_inicio_compra and data_fim_compra:
        conditions.append(f"data_ultima_compra >= '{data_inicio_compra}'")
        conditions.append(f"data_ultima_compra <= '{data_fim_compra}'")
    
    return " AND ".join(conditions) if conditions else "1=1"

# ==========================================
# ANÁLISE AUTOMÁTICA (SEMPRE EXECUTA)
# ==========================================
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
    COALESCE(MIN(data_ultima_visita), CURRENT_DATE) as primeira_visita,
    COALESCE(MAX(data_ultima_visita), CURRENT_DATE) as ultima_visita
FROM filtered
"""

result = con.execute(analysis_query).fetchone()
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
    try:
        periodo_filtrado = f"{primeira_visita.date()} a {ultima_visita.date()}"
    except:
        periodo_filtrado = "Período disponível"
    
    st.metric("Período Filtrado", periodo_filtrado)
    st.markdown('</div>', unsafe_allow_html=True)

# ==========================================
# PRÉ-VISUALIZAÇÃO DOS DADOS
# ==========================================
if total_filtrado > 0:
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
            date_cols = ['data_ultima_visita', 'data_ultima_compra']
            for col in date_cols:
                if col in preview_df.columns:
                    preview_df[col] = pd.to_datetime(preview_df[col], errors='coerce')
            
            st.dataframe(
                preview_df,
                use_container_width=True,
                column_config={
                    "member_pk": "ID Cliente",
                    "categoria": "Categoria",
                    "setor": "Setor",
                    "data_ultima_visita": st.column_config.DatetimeColumn("Última Visita", format="DD/MM/YYYY"),
                    "data_ultima_compra": st.column_config.DatetimeColumn("Última Compra", format="DD/MM/YYYY")
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

# ==========================================
# RODAPÉ
# ==========================================
st.markdown("---")
st.caption(f"📊 Base de dados: {dataset_info['num_rows']:,} registros • Última atualização: {datetime.now().strftime('%d/%m/%Y %H:%M')}")
