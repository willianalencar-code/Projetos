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
    try:
        token = st.secrets.get("HF_TOKEN", "")
        caminho_local = hf_hub_download(
            repo_id="WillianAlencar/SegmentacaoClientes",
            filename="dataset.parquet",
            repo_type="dataset",
            token=token if token else None
        )

        parquet_file = pq.ParquetFile(caminho_local)
        num_rows = parquet_file.metadata.num_rows

        con = duckdb.connect(database=':memory:')

        categorias = con.execute(
            f"SELECT DISTINCT categoria FROM read_parquet('{caminho_local}') WHERE categoria IS NOT NULL"
        ).df()["categoria"].tolist()

        setores = con.execute(
            f"SELECT DISTINCT setor FROM read_parquet('{caminho_local}') WHERE setor IS NOT NULL"
        ).df()["setor"].tolist()

        dates_df = con.execute(f"""
            SELECT 
                MIN(data_ultima_visita) as min_visita,
                MAX(data_ultima_visita) as max_visita,
                MIN(data_ultima_compra) as min_compra,
                MAX(data_ultima_compra) as max_compra,
                MIN(data_cadastro) as min_cadastro,
                MAX(data_cadastro) as max_cadastro
            FROM read_parquet('{caminho_local}')
        """).df()

        schema_df = con.execute(
            f"DESCRIBE SELECT * FROM read_parquet('{caminho_local}')"
        ).df()

        con.close()

        return {
            'caminho': caminho_local,
            'num_rows': num_rows,
            'categorias': sorted(categorias),
            'setores': sorted(setores),
            'min_visita': dates_df['min_visita'].iloc[0] or pd.Timestamp('2020-01-01'),
            'max_visita': dates_df['max_visita'].iloc[0] or pd.Timestamp.now(),
            'min_compra': dates_df['min_compra'].iloc[0] or pd.Timestamp('2020-01-01'),
            'max_compra': dates_df['max_compra'].iloc[0] or pd.Timestamp.now(),
            'min_cadastro': dates_df['min_cadastro'].iloc[0] or pd.Timestamp('2020-01-01'),
            'max_cadastro': dates_df['max_cadastro'].iloc[0] or pd.Timestamp.now(),
            'has_flg_premium': 'flg_premium_ativo' in schema_df['column_name'].values
        }

    except Exception as e:
        st.error(f"Erro de conexão: {e}")
        return None

# ==========================================
# CABEÇALHO
# ==========================================
st.markdown("# 👥 Segmentação de Clientes")
st.markdown("**Filtre e exporte sua base de clientes**")

with st.spinner("📦 Carregando informações..."):
    dataset_info = get_dataset_info()

if not dataset_info:
    st.stop()

# ==========================================
# SIDEBAR - FILTROS
# ==========================================
with st.sidebar:
    st.markdown("### 🔍 Filtros Avançados")

    id_busca = st.text_input("Buscar Cliente (IDs separados por vírgula)")

    cat_sel = st.multiselect("Categorias", dataset_info['categorias'])
    setor_sel = st.multiselect("Setores", dataset_info['setores'])

    apenas_sem_compra = st.checkbox("Apenas clientes sem compra")

    excluir_premium = False
    if dataset_info['has_flg_premium']:
        excluir_premium = st.checkbox("Excluir clientes premium")

    st.divider()

    usar_visita = st.toggle("Filtrar última visita", value=True)
    if usar_visita:
        col1, col2 = st.columns(2)
        with col1:
            data_inicio_visita = st.date_input(
                "De",
                dataset_info['min_visita'].date(),
                key="visita_ini"
            )
        with col2:
            data_fim_visita = st.date_input(
                "Até",
                dataset_info['max_visita'].date(),
                key="visita_fim"
            )

    usar_compra = st.toggle("Filtrar última compra", value=False)
    if usar_compra and not apenas_sem_compra:
        col3, col4 = st.columns(2)
        with col3:
            data_inicio_compra = st.date_input(
                "De",
                dataset_info['min_compra'].date(),
                key="compra_ini"
            )
        with col4:
            data_fim_compra = st.date_input(
                "Até",
                dataset_info['max_compra'].date(),
                key="compra_fim"
            )
    else:
        usar_compra = False

    usar_cadastro = st.toggle("Filtrar data de cadastro", value=False)
    if usar_cadastro:
        col5, col6 = st.columns(2)
        with col5:
            data_inicio_cadastro = st.date_input(
                "De",
                dataset_info['min_cadastro'].date(),
                key="cad_ini"
            )
        with col6:
            data_fim_cadastro = st.date_input(
                "Até",
                dataset_info['max_cadastro'].date(),
                key="cad_fim"
            )

    st.divider()
    aplicar_filtros = st.button("🔎 Aplicar filtros", use_container_width=True)

# ==========================================
# QUERY BUILDER (AJUSTADA, NÃO REESCRITA)
# ==========================================
def build_query_conditions():
    conditions = []

    if id_busca:
        ids = [i.strip() for i in id_busca.split(",") if i.strip()]
        ids_sql = ", ".join([f"'{i}'" for i in ids])
        conditions.append(f"member_pk IN ({ids_sql})")

    if cat_sel:
        cat_sql = ", ".join([f"'{c}'" for c in cat_sel])
        conditions.append(f"categoria IN ({cat_sql})")

    if setor_sel:
        setor_sql = ", ".join([f"'{s}'" for s in setor_sel])
        conditions.append(f"setor IN ({setor_sql})")

    if usar_visita:
        conditions.append(
            f"data_ultima_visita BETWEEN '{data_inicio_visita}' AND '{data_fim_visita}'"
        )

    if apenas_sem_compra:
        conditions.append("data_ultima_compra IS NULL")
    elif usar_compra:
        conditions.append(
            f"data_ultima_compra BETWEEN '{data_inicio_compra}' AND '{data_fim_compra}'"
        )

    if usar_cadastro:
        conditions.append(
            f"data_cadastro BETWEEN '{data_inicio_cadastro}' AND '{data_fim_cadastro}'"
        )

    if excluir_premium and dataset_info['has_flg_premium']:
        conditions.append("(flg_premium_ativo = 'N' OR flg_premium_ativo IS NULL)")

    return " AND ".join(conditions) if conditions else "1=1"

# ==========================================
# EXECUÇÃO
# ==========================================
if aplicar_filtros:
    con = duckdb.connect(database=':memory:')
    where_clause = build_query_conditions()

    total, clientes, com_compra, sem_compra = con.execute(f"""
        SELECT
            COUNT(*) total,
            COUNT(DISTINCT member_pk) clientes,
            COUNT(DISTINCT CASE WHEN data_ultima_compra IS NOT NULL THEN member_pk END) com_compra,
            COUNT(DISTINCT CASE WHEN data_ultima_compra IS NULL THEN member_pk END) sem_compra
        FROM read_parquet('{dataset_info['caminho']}')
        WHERE {where_clause}
    """).fetchone()

    st.markdown("### 📊 Resultados")

    c1, c2, c3 = st.columns(3)
    c1.metric("Registros", f"{total:,}")
    c2.metric("Clientes", f"{clientes:,}")
    c3.metric("Sem compra", f"{sem_compra:,}")

    with st.expander("👁️ Pré-visualização", expanded=True):
        df = con.execute(f"""
            SELECT *
            FROM read_parquet('{dataset_info['caminho']}')
            WHERE {where_clause}
            LIMIT 100
        """).df()

        st.dataframe(df, use_container_width=True, hide_index=True)

    con.close()

# ==========================================
# RODAPÉ
# ==========================================
st.markdown("---")
st.caption(
    f"📊 Base: {dataset_info['num_rows']:,} registros • "
    f"Atualizado em {datetime.now().strftime('%d/%m/%Y %H:%M')}"
)
