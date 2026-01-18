import streamlit as st
import duckdb
from huggingface_hub import hf_hub_download
import pandas as pd
import altair as alt
import io

# ==========================================
# 1. CONFIGURAÇÃO DA PÁGINA
# ==========================================
st.set_page_config(
    page_title="Sistema de Clientes 7M",
    layout="wide",
    page_icon="📊"
)

# ==========================================
# 2. CACHE E CONEXÃO
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
# 3. INICIALIZAÇÃO
# ==========================================
caminho_arquivo = get_dataset()
con = get_connection()

if caminho_arquivo:
    # Cria tabela temporária no DuckDB
    con.execute(f"CREATE OR REPLACE TABLE clientes AS SELECT * FROM read_parquet('{caminho_arquivo}')")
    
    st.title("📊 Gestão de Clientes - 7 Milhões")
    
    # ==========================================
    # 4. SIDEBAR - FILTROS E EXPORTAÇÃO
    # ==========================================
    st.sidebar.header("🔍 Filtros Dinâmicos")
    
    # --- Widgets usando session_state com key ---
    id_busca = st.sidebar.text_input(
        "Buscar por member_pk:",
        key="id_busca"
    )

    categorias = con.execute("SELECT DISTINCT categoria FROM clientes WHERE categoria IS NOT NULL").df()['categoria'].tolist()
    cat_sel = st.sidebar.multiselect(
        "Categorias:",
        categorias,
        key="cat_sel"
    )

    setores = con.execute("SELECT DISTINCT setor FROM clientes WHERE setor IS NOT NULL").df()['setor'].tolist()
    setor_sel = st.sidebar.multiselect(
        "Setores:",
        setores,
        key="setor_sel"
    )

    # Para date_input, precisamos lidar de forma diferente
    min_data_visita = con.execute("SELECT MIN(data_ultima_visita) FROM clientes").fetchone()[0]
    max_data_visita = con.execute("SELECT MAX(data_ultima_visita) FROM clientes").fetchone()[0]
    
    min_data_compra = con.execute("SELECT MIN(data_ultima_compra) FROM clientes").fetchone()[0]
    max_data_compra = con.execute("SELECT MAX(data_ultima_compra) FROM clientes").fetchone()[0]

    # Use session_state para manter os valores dos date_input
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
    
    # ==========================================
    # 5. NOVA SEÇÃO: EXPORTAÇÃO COMPLETA
    # ==========================================
    st.sidebar.header("📤 Exportação de Dados")
    
    # Opções de exportação
    export_option = st.sidebar.radio(
        "Escolha o que exportar:",
        ["Amostra (1.000 linhas)", "Dados Filtrados", "Dataset Completo"],
        key="export_option"
    )
    
    # Formato de exportação
    export_format = st.sidebar.selectbox(
        "Formato de exportação:",
        ["CSV", "Parquet", "Excel"],
        key="export_format"
    )
    
    # Nome do arquivo
    default_filename = "clientes"
    custom_filename = st.sidebar.text_input(
        "Nome do arquivo (sem extensão):",
        value=default_filename,
        key="export_filename"
    )
    
    # ==========================================
    # 6. QUERY E EXIBIÇÃO DOS RESULTADOS
    # ==========================================
    # --- Monta query dinâmica ---
    query = "SELECT * FROM clientes WHERE 1=1"

    if id_busca:
        query += f" AND CAST(member_pk AS VARCHAR) LIKE '%{id_busca}%'"

    if cat_sel:
        placeholders = ', '.join([f"'{c}'" for c in cat_sel])
        query += f" AND categoria IN ({placeholders})"

    if setor_sel:
        placeholders = ', '.join([f"'{s}'" for s in setor_sel])
        query += f" AND setor IN ({placeholders})"

    if len(date_visita_range) == 2:
        start, end = date_visita_range
        query += f" AND data_ultima_visita BETWEEN '{start}' AND '{end}'"
        # Atualiza session_state
        st.session_state.date_visita = date_visita_range

    if len(date_compra_range) == 2:
        start, end = date_compra_range
        query += f" AND data_ultima_compra BETWEEN '{start}' AND '{end}'"
        # Atualiza session_state
        st.session_state.date_compra = date_compra_range

    # --- Processamento ---
    with st.spinner("Processando filtros..."):
        total = con.execute(f"SELECT COUNT(*) FROM ({query})").fetchone()[0]
        total_unicos = con.execute(f"SELECT COUNT(DISTINCT member_pk) FROM ({query})").fetchone()[0]
        
        # Para visualização, sempre mostra apenas 1000 linhas
        df_result = con.execute(query + " LIMIT 1000").df()
        for col in ['data_ultima_visita', 'data_ultima_compra']:
            if col in df_result.columns:
                df_result[col] = pd.to_datetime(df_result[col], errors='coerce')

    # --- Métricas ---
    c1, c2, c3 = st.columns(3)
    c1.metric("Total de Registros", f"{total:,}")
    c2.metric("Clientes Únicos", f"{total_unicos:,}")
    
    # Mostrar o tipo de exportação selecionado
    export_info = {
        "Amostra (1.000 linhas)": "1.000 registros",
        "Dados Filtrados": f"{total:,} registros",
        "Dataset Completo": "7M registros (completo)"
    }
    c3.metric("Exportação Selecionada", export_info[export_option])

    # --- Gráficos ---
    col_a, col_b = st.columns(2)
    with col_a:
        st.subheader("Volume por Setor")
        df_graf_setor = con.execute(f"""
            SELECT setor, COUNT(*) AS total
            FROM ({query})
            GROUP BY setor
            ORDER BY total DESC
            LIMIT 10
        """).df()
        if not df_graf_setor.empty:
            chart = alt.Chart(df_graf_setor).mark_bar().encode(
                x=alt.X('setor', sort='-y'),
                y='total',
                tooltip=['setor', 'total']
            )
            st.altair_chart(chart, use_container_width=True)

    with col_b:
        st.subheader("Volume por Categoria")
        df_graf_cat = con.execute(f"""
            SELECT categoria, COUNT(*) AS total
            FROM ({query})
            GROUP BY categoria
            ORDER BY total DESC
        """).df()
        if not df_graf_cat.empty:
            chart = alt.Chart(df_graf_cat).mark_bar().encode(
                x=alt.X('categoria', sort='-y'),
                y='total',
                tooltip=['categoria', 'total']
            )
            st.altair_chart(chart, use_container_width=True)

    # --- Tabela ---
    st.subheader("📋 Detalhes da Amostra (1.000 linhas)")
    st.dataframe(df_result, use_container_width=True)
    
    # ==========================================
    # 7. EXPORTAÇÃO AVANÇADA
    # ==========================================
    st.subheader("📤 Exportação de Dados")
    
    # Container para exportação
    with st.container():
        col_exp1, col_exp2, col_exp3 = st.columns(3)
        
        with col_exp1:
            st.info(f"**Opção selecionada:** {export_option}")
            
        with col_exp2:
            st.info(f"**Formato:** {export_format}")
            
        with col_exp3:
            st.info(f"**Arquivo:** {custom_filename}.{export_format.lower()}")
        
        # Botão de exportação
        export_button = st.button(
            "🚀 Iniciar Exportação",
            type="primary",
            use_container_width=True
        )
        
        if export_button:
            with st.spinner(f"Preparando exportação de {export_option}..."):
                try:
                    # Determinar qual query usar baseado na opção selecionada
                    if export_option == "Amostra (1.000 linhas)":
                        export_query = query + " LIMIT 1000"
                        filename_suffix = "_amostra"
                    elif export_option == "Dados Filtrados":
                        export_query = query
                        filename_suffix = "_filtrado"
                    else:  # Dataset Completo
                        export_query = "SELECT * FROM clientes"
                        filename_suffix = "_completo"
                    
                    # Executar a query
                    df_export = con.execute(export_query).df()
                    
                    # Converter datas
                    for col in ['data_ultima_visita', 'data_ultima_compra']:
                        if col in df_export.columns:
                            df_export[col] = pd.to_datetime(df_export[col], errors='coerce')
                    
                    st.success(f"✅ Dados preparados: {len(df_export):,} registros")
                    
                    # Preparar arquivo baseado no formato selecionado
                    if export_format == "CSV":
                        # Para CSV grande, usar buffer de memória
                        csv_data = df_export.to_csv(index=False).encode('utf-8')
                        file_extension = "csv"
                        mime_type = "text/csv"
                        
                        st.download_button(
                            label=f"📥 Baixar {export_format} ({len(df_export):,} registros)",
                            data=csv_data,
                            file_name=f"{custom_filename}{filename_suffix}.{file_extension}",
                            mime=mime_type,
                            use_container_width=True
                        )
                    
                    elif export_format == "Parquet":
                        # Parquet é mais eficiente para grandes volumes
                        buffer = io.BytesIO()
                        df_export.to_parquet(buffer, index=False)
                        buffer.seek(0)
                        file_extension = "parquet"
                        mime_type = "application/octet-stream"
                        
                        st.download_button(
                            label=f"📥 Baixar {export_format} ({len(df_export):,} registros)",
                            data=buffer,
                            file_name=f"{custom_filename}{filename_suffix}.{file_extension}",
                            mime=mime_type,
                            use_container_width=True
                        )
                    
                    elif export_format == "Excel":
                        # Para Excel, limitar a 1M linhas (limitação do Excel)
                        max_excel_rows = 1000000
                        if len(df_export) > max_excel_rows:
                            st.warning(f"⚠️ Excel suporta até 1M linhas. Serão exportadas {max_excel_rows:,} linhas.")
                            df_export = df_export.head(max_excel_rows)
                        
                        buffer = io.BytesIO()
                        with pd.ExcelWriter(buffer, engine='openpyxl') as writer:
                            df_export.to_excel(writer, index=False, sheet_name='Clientes')
                        buffer.seek(0)
                        file_extension = "xlsx"
                        mime_type = "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
                        
                        st.download_button(
                            label=f"📥 Baixar {export_format} ({len(df_export):,} registros)",
                            data=buffer,
                            file_name=f"{custom_filename}{filename_suffix}.{file_extension}",
                            mime=mime_type,
                            use_container_width=True
                        )
                    
                    # Informações adicionais
                    with st.expander("📊 Estatísticas da Exportação"):
                        st.write(f"**Total de registros exportados:** {len(df_export):,}")
                        st.write(f"**Colunas exportadas:** {', '.join(df_export.columns.tolist())}")
                        st.write(f"**Tamanho estimado:** {len(df_export) * 100 / 1024 / 1024:.2f} MB (aproximado)")
                        
                except Exception as e:
                    st.error(f"❌ Erro durante a exportação: {str(e)}")
    
    # ==========================================
    # 8. DICAS DE EXPORTAÇÃO
    # ==========================================
    with st.expander("💡 Dicas para Exportação"):
        st.markdown("""
        **Para grandes volumes de dados:**
        1. **Parquet** é o formato mais eficiente (menor tamanho, mais rápido)
        2. **CSV** é universal mas pode ser muito grande para 7M registros
        3. **Excel** tem limite de ~1M linhas por planilha
        
        **Recomendações:**
        - Para análise local: use **Parquet** com pandas ou DuckDB
        - Para compartilhar: use **CSV** se for menos de 100K linhas
        - Para relatórios: use **Excel** com filtros aplicados
        
        **Atenção:** A exportação do dataset completo (7M registros) pode:
        - Demorar vários minutos
        - Gerar arquivo de vários GBs
        - Consumir muita memória no navegador
        """)

else:
    st.warning("Aguardando configuração do Token nos Secrets do Streamlit Cloud.")
