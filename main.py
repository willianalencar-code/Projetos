import streamlit as st
import duckdb
from huggingface_hub import hf_hub_download
import pandas as pd
import altair as alt
from datetime import datetime, timedelta

# ==========================================
# 1. CONFIGURAÇÃO DA PÁGINA
# ==========================================
st.set_page_config(
    page_title="Sistema de CRM - Clientes 7M",
    layout="wide",
    page_icon="📊"
)

# ==========================================
# 2. FUNÇÕES AUXILIARES
# ==========================================
def calcular_segmento_engajamento(row):
    """Classifica clientes por engajamento"""
    hoje = datetime.now().date()
    
    if pd.isna(row['data_ultima_compra']):
        return 'Nunca Comprou'
    
    dias_sem_compra = (hoje - row['data_ultima_compra'].date()).days
    
    if dias_sem_compra <= 30:
        return 'Ativo'
    elif dias_sem_compra <= 90:
        return 'Em Risco'
    elif dias_sem_compra <= 180:
        return 'Inativo Recente'
    else:
        return 'Inativo Crônico'

def calcular_tempo_visita_compra(row):
    """Calcula dias entre última visita e última compra"""
    if pd.isna(row['data_ultima_visita']) or pd.isna(row['data_ultima_compra']):
        return None
    return (row['data_ultima_compra'] - row['data_ultima_visita']).days

# ==========================================
# 3. CACHE E CONEXÃO
# ==========================================
@st.cache_data(show_spinner=False)
def get_dataset():
    """Baixa o arquivo do HF para o cache local"""
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
# 4. INICIALIZAÇÃO
# ==========================================
caminho_arquivo = get_dataset()
con = get_connection()

if caminho_arquivo:
    # Cria tabela temporária no DuckDB
    con.execute(f"CREATE OR REPLACE TABLE clientes AS SELECT * FROM read_parquet('{caminho_arquivo}')")
    
    st.title("📊 Sistema de CRM - Gestão de Clientes")
    
    # ==========================================
    # 5. SIDEBAR - FILTROS E CONFIGURAÇÕES
    # ==========================================
    st.sidebar.header("🔍 Filtros Dinâmicos")
    
    # Configuração de períodos
    st.sidebar.subheader("⚙️ Configuração de Segmentação")
    dias_ativos = st.sidebar.slider("Dias para considerar Ativo", 1, 90, 30)
    dias_risco = st.sidebar.slider("Dias para considerar Em Risco", 31, 180, 90)
    dias_inativo = st.sidebar.slider("Dias para considerar Inativo", 91, 365, 180)
    
    # Widgets de filtro
    id_busca = st.sidebar.text_input("Buscar por member_pk:", key="id_busca")
    
    categorias = con.execute("SELECT DISTINCT categoria FROM clientes WHERE categoria IS NOT NULL").df()['categoria'].tolist()
    cat_sel = st.sidebar.multiselect("Categorias:", categorias, key="cat_sel")
    
    setores = con.execute("SELECT DISTINCT setor FROM clientes WHERE setor IS NOT NULL").df()['setor'].tolist()
    setor_sel = st.sidebar.multiselect("Setores:", setores, key="setor_sel")
    
    # Filtros de data
    min_data_visita = con.execute("SELECT MIN(data_ultima_visita) FROM clientes").fetchone()[0]
    max_data_visita = con.execute("SELECT MAX(data_ultima_visita) FROM clientes").fetchone()[0]
    min_data_compra = con.execute("SELECT MIN(data_ultima_compra) FROM clientes").fetchone()[0]
    max_data_compra = con.execute("SELECT MAX(data_ultima_compra) FROM clientes").fetchone()[0]
    
    # Inicialização do session_state para datas
    if "date_visita" not in st.session_state:
        st.session_state.date_visita = [min_data_visita, max_data_visita]
    if "date_compra" not in st.session_state:
        st.session_state.date_compra = [min_data_compra, max_data_compra]
    
    date_visita_range = st.sidebar.date_input(
        "Período da última visita",
        value=st.session_state.date_visita,
        key="date_visita_input"
    )
    
    date_compra_range = st.sidebar.date_input(
        "Período da última compra",
        value=st.session_state.date_compra,
        key="date_compra_input"
    )
    
    # Filtro de segmento de engajamento
    segmentos_opcoes = ['Ativo', 'Em Risco', 'Inativo Recente', 'Inativo Crônico', 'Nunca Comprou', 'Todos']
    segmento_sel = st.sidebar.multiselect(
        "Segmento de Engajamento:",
        segmentos_opcoes,
        default=['Todos'],
        key="segmento_sel"
    )
    
    # ==========================================
    # 6. QUERY E PROCESSAMENTO
    # ==========================================
    # Monta query dinâmica
    query_base = "SELECT * FROM clientes WHERE 1=1"
    
    if id_busca:
        query_base += f" AND CAST(member_pk AS VARCHAR) LIKE '%{id_busca}%'"
    
    if cat_sel:
        placeholders = ', '.join([f"'{c}'" for c in cat_sel])
        query_base += f" AND categoria IN ({placeholders})"
    
    if setor_sel:
        placeholders = ', '.join([f"'{s}'" for s in setor_sel])
        query_base += f" AND setor IN ({placeholders})"
    
    if len(date_visita_range) == 2:
        start, end = date_visita_range
        query_base += f" AND data_ultima_visita BETWEEN '{start}' AND '{end}'"
        st.session_state.date_visita = date_visita_range
    
    if len(date_compra_range) == 2:
        start, end = date_compra_range
        query_base += f" AND data_ultima_compra BETWEEN '{start}' AND '{end}'"
        st.session_state.date_compra = date_compra_range
    
    # Processamento dos dados
    with st.spinner("Processando análise de CRM..."):
        df_result = con.execute(query_base + " LIMIT 5000").df()
        
        # Converter datas
        for col in ['data_ultima_visita', 'data_ultima_compra']:
            if col in df_result.columns:
                df_result[col] = pd.to_datetime(df_result[col], errors='coerce')
        
        # Calcular métricas de CRM
        df_result['segmento_engajamento'] = df_result.apply(calcular_segmento_engajamento, axis=1)
        df_result['dias_ultima_compra'] = (datetime.now().date() - df_result['data_ultima_compra'].dt.date).dt.days
        df_result['tempo_visita_compra'] = df_result.apply(calcular_tempo_visita_compra, axis=1)
        
        # Aplicar filtro de segmento se não for "Todos"
        if 'Todos' not in segmento_sel and segmento_sel:
            df_result = df_result[df_result['segmento_engajamento'].isin(segmento_sel)]
        
        total = len(df_result)
        total_unicos = df_result['member_pk'].nunique()
    
    # ==========================================
    # 7. PAINEL DE KPIs E MÉTRICAS
    # ==========================================
    st.header("📈 Painel de CRM - Indicadores Principais")
    
    # Métricas gerais
    col1, col2, col3, col4 = st.columns(4)
    with col1:
        st.metric("Total de Clientes", f"{total_unicos:,}")
    with col2:
        taxa_conversao = df_result[df_result['data_ultima_compra'].notna()].shape[0] / max(total, 1) * 100
        st.metric("Taxa de Conversão", f"{taxa_conversao:.1f}%")
    with col3:
        tempo_medio = df_result['tempo_visita_compra'].mean()
        st.metric("Tempo Média Visita→Compra", f"{tempo_medio:.1f} dias" if pd.notna(tempo_medio) else "N/A")
    with col4:
        clientes_ativos = df_result[df_result['segmento_engajamento'] == 'Ativo'].shape[0]
        st.metric("Clientes Ativos", f"{clientes_ativos:,}")
    
    # ==========================================
    # 8. VISUALIZAÇÕES DE SEGMENTAÇÃO
    # ==========================================
    st.subheader("🎯 Segmentação por Engajamento")
    
    col_a, col_b = st.columns(2)
    
    with col_a:
        # Distribuição por segmento
        df_segmento = df_result['segmento_engajamento'].value_counts().reset_index()
        df_segmento.columns = ['Segmento', 'Quantidade']
        
        if not df_segmento.empty:
            chart_segmento = alt.Chart(df_segmento).mark_bar().encode(
                x=alt.X('Segmento', sort='-y'),
                y='Quantidade',
                color=alt.Color('Segmento', scale=alt.Scale(scheme='category10')),
                tooltip=['Segmento', 'Quantidade']
            ).properties(height=300)
            st.altair_chart(chart_segmento, use_container_width=True)
    
    with col_b:
        # Distribuição por recência de compra
        df_recencia = pd.cut(df_result['dias_ultima_compra'], 
                           bins=[0, 30, 90, 180, 365, float('inf')],
                           labels=['0-30 dias', '31-90 dias', '91-180 dias', '181-365 dias', '+365 dias'])
        df_recencia_counts = df_recencia.value_counts().reset_index()
        df_recencia_counts.columns = ['Faixa Recência', 'Quantidade']
        
        if not df_recencia_counts.empty:
            chart_recencia = alt.Chart(df_recencia_counts).mark_arc().encode(
                theta='Quantidade',
                color=alt.Color('Faixa Recência', scale=alt.Scale(scheme='set2')),
                tooltip=['Faixa Recência', 'Quantidade']
            ).properties(height=300)
            st.altair_chart(chart_recencia, use_container_width=True)
    
    # ==========================================
    # 9. ANÁLISE POR SETOR/CATEGORIA
    # ==========================================
    st.subheader("🏢 Análise por Setor e Categoria")
    
    col_c, col_d = st.columns(2)
    
    with col_c:
        # Setor x Segmento (heatmap)
        df_setor_segmento = df_result.groupby(['setor', 'segmento_engajamento']).size().reset_index(name='count')
        
        if not df_setor_segmento.empty:
            heatmap = alt.Chart(df_setor_segmento).mark_rect().encode(
                x='setor:N',
                y='segmento_engajamento:N',
                color='count:Q',
                tooltip=['setor', 'segmento_engajamento', 'count']
            ).properties(height=300)
            st.altair_chart(heatmap, use_container_width=True)
    
    with col_d:
        # Categoria x Recência
        df_cat_recencia = df_result.groupby('categoria').agg({
            'dias_ultima_compra': 'mean',
            'member_pk': 'count'
        }).reset_index()
        df_cat_recencia.columns = ['Categoria', 'Dias Média Sem Compra', 'Total Clientes']
        
        if not df_cat_recencia.empty:
            scatter = alt.Chart(df_cat_recencia).mark_circle(size=100).encode(
                x='Dias Média Sem Compra',
                y='Categoria',
                size='Total Clientes',
                color='Total Clientes',
                tooltip=['Categoria', 'Dias Média Sem Compra', 'Total Clientes']
            ).properties(height=300)
            st.altair_chart(scatter, use_container_width=True)
    
    # ==========================================
    # 10. PAINEL DE AÇÕES ACIONÁVEIS
    # ==========================================
    st.subheader("🚀 Painel de Ações de CRM")
    
    # Criar abas para diferentes ações
    tab1, tab2, tab3, tab4 = st.tabs([
        "📧 Recuperação", 
        "💰 Upsell", 
        "🔄 Reativação", 
        "🎯 Segmentação Avançada"
    ])
    
    with tab1:
        st.info("**Clientes para Recuperação** - Última compra entre 90 e 180 dias")
        df_recuperacao = df_result[
            (df_result['dias_ultima_compra'] > 90) & 
            (df_result['dias_ultima_compra'] <= 180)
        ]
        st.dataframe(df_recuperacao[['member_pk', 'setor', 'categoria', 'dias_ultima_compra']].head(20))
        st.caption(f"Total de clientes para recuperação: {len(df_recuperacao):,}")
        
    with tab2:
        st.success("**Clientes para Upsell** - Ativos (≤30 dias) por setor")
        df_upsell = df_result[df_result['segmento_engajamento'] == 'Ativo']
        if not df_upsell.empty:
            df_upsell_group = df_upsell.groupby(['setor', 'categoria']).size().reset_index(name='clientes')
            st.dataframe(df_upsell_group.sort_values('clientes', ascending=False))
        else:
            st.write("Nenhum cliente ativo encontrado")
            
    with tab3:
        st.warning("**Clientes para Reativação** - Inativos Crônicos (>180 dias)")
        df_reativacao = df_result[df_result['segmento_engajamento'] == 'Inativo Crônico']
        st.dataframe(df_reativacao[['member_pk', 'setor', 'categoria', 'dias_ultima_compra']].head(20))
        st.caption(f"Total de clientes inativos crônicos: {len(df_reativacao):,}")
        
    with tab4:
        st.info("**Visitas sem Compra Recente** - Visitaram mas não compraram nos últimos 30 dias")
        df_visita_sem_compra = df_result[
            (df_result['data_ultima_visita'].notna()) &
            ((df_result['dias_ultima_compra'] > 30) | (df_result['data_ultima_compra'].isna()))
        ]
        st.dataframe(df_visita_sem_compra[['member_pk', 'setor', 'categoria', 'dias_ultima_compra']].head(20))
        st.caption(f"Total de visitas sem compra recente: {len(df_visita_sem_compra):,}")
    
    # ==========================================
    # 11. RESUMO EXECUTIVO
    # ==========================================
    st.subheader("📋 Resumo Executivo")
    
    # Criar um resumo tabular
    resumo_data = []
    for setor in df_result['setor'].unique():
        subset = df_result[df_result['setor'] == setor]
        if len(subset) > 0:
            resumo_data.append({
                'Setor': setor,
                'Total Clientes': len(subset),
                '% Ativos': (subset['segmento_engajamento'] == 'Ativo').sum() / len(subset) * 100,
                'Dias Média Sem Compra': subset['dias_ultima_compra'].mean(),
                'Ação Sugerida': 'Upsell' if (subset['segmento_engajamento'] == 'Ativo').sum() / len(subset) > 0.5 else 'Recuperação'
            })
    
    if resumo_data:
        df_resumo = pd.DataFrame(resumo_data)
        st.dataframe(df_resumo.sort_values('Total Clientes', ascending=False), use_container_width=True)
    
    # ==========================================
    # 12. DETALHES E EXPORTAÇÃO
    # ==========================================
    st.subheader("📋 Detalhes da Base Filtrada")
    
    # Mostrar estatísticas
    with st.expander("📊 Estatísticas Detalhadas"):
        st.write(f"**Distribuição por Segmento:**")
        st.write(df_result['segmento_engajamento'].value_counts())
        
        st.write(f"**Distribuição por Setor:**")
        st.write(df_result['setor'].value_counts())
        
        st.write(f"**Estatísticas de Recência:**")
        st.write(df_result['dias_ultima_compra'].describe())
    
    # Tabela com dados
    st.dataframe(df_result.head(1000), use_container_width=True)
    
    # Exportação
    csv = df_result.to_csv(index=False).encode('utf-8')
    st.download_button(
        "📥 Exportar Dados Completos CSV",
        csv,
        f"crm_clientes_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv",
        "text/csv"
    )
    
    # ==========================================
    # 13. RECOMENDAÇÕES
    # ==========================================
    st.sidebar.markdown("---")
    st.sidebar.subheader("💡 Recomendações de Ações")
    
    # Análise automática de recomendações
    if total_unicos > 0:
        perc_ativos = (df_result['segmento_engajamento'] == 'Ativo').sum() / total * 100
        perc_inativos = (df_result['segmento_engajamento'] == 'Inativo Crônico').sum() / total * 100
        
        if perc_ativos < 20:
            st.sidebar.error(f"⚠️ **Alerta**: Apenas {perc_ativos:.1f}% dos clientes estão ativos!")
            st.sidebar.info("**Ação sugerida**: Focar em campanhas de reativação")
        elif perc_inativos > 30:
            st.sidebar.warning(f"⚠️ **Atenção**: {perc_inativos:.1f}% dos clientes são inativos crônicos")
            st.sidebar.info("**Ação sugerida**: Implementar programa de fidelidade")
        else:
            st.sidebar.success(f"✅ **Status Bom**: {perc_ativos:.1f}% de clientes ativos")
            st.sidebar.info("**Ação sugerida**: Campanhas de upsell/cross-sell")

else:
    st.warning("Aguardando configuração do Token nos Secrets do Streamlit Cloud.")
