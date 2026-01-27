import streamlit as st
import duckdb
from huggingface_hub import hf_hub_download
import pandas as pd
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
# CACHE - INFO DO DATASET
# ==========================================
@st.cache_data(show_spinner=False, ttl=3600)
def get_dataset_info():
    token = st.secrets.get("HF_TOKEN", "")
    path = hf_hub_download(
        repo_id="WillianAlencar/SegmentacaoClientes",
        filename="dataset.parquet",
        repo_type="dataset",
        token=token if token else None
    )

    parquet = pq.ParquetFile(path)
    num_rows = parquet.metadata.num_rows

    con = duckdb.connect(database=":memory:")

    categorias = con.execute(
        f"SELECT DISTINCT categoria FROM read_parquet('{path}') WHERE categoria IS NOT NULL"
    ).df()["categoria"].tolist()

    setores = con.execute(
        f"SELECT DISTINCT setor FROM read_parquet('{path}') WHERE setor IS NOT NULL"
    ).df()["setor"].tolist()

    dates = con.execute(f"""
        SELECT
            MIN(data_ultima_visita) min_visita,
            MAX(data_ultima_visita) max_visita,
            MIN(data_ultima_compra) min_compra,
            MAX(data_ultima_compra) max_compra,
            MIN(data_cadastro) min_cadastro,
            MAX(data_cadastro) max_cadastro
        FROM read_parquet('{path}')
    """).df().iloc[0]

    schema = con.execute(
        f"DESCRIBE SELECT * FROM read_parquet('{path}')"
    ).df()

    con.close()

    return {
        "path": path,
        "num_rows": num_rows,
        "categorias": sorted(categorias),
        "setores": sorted(setores),
        "min_visita": dates["min_visita"] or pd.Timestamp("2020-01-01"),
        "max_visita": dates["max_visita"] or pd.Timestamp.now(),
        "min_compra": dates["min_compra"] or pd.Timestamp("2020-01-01"),
        "max_compra": dates["max_compra"] or pd.Timestamp.now(),
        "min_cadastro": dates["min_cadastro"] or pd.Timestamp("2020-01-01"),
        "max_cadastro": dates["max_cadastro"] or pd.Timestamp.now(),
        "has_premium": "flg_premium_ativo" in schema["column_name"].values
    }

# ==========================================
# CABEÇALHO
# ==========================================
st.markdown("# 👥 Segmentação de Clientes")
st.markdown("**Filtre, analise e exporte sua base de clientes**")

dataset = get_dataset_info()

# ==========================================
# SIDEBAR - FILTROS
# ==========================================
with st.sidebar:
    st.markdown("### 🔍 Filtros")

    id_busca = st.text_input("Buscar IDs (separados por vírgula)")

    cat_sel = st.multiselect("Categorias", dataset["categorias"])
    setor_sel = st.multiselect("Setores", dataset["setores"])

    apenas_sem_compra = st.checkbox("Apenas clientes sem compra")

    excluir_premium = False
    if dataset["has_premium"]:
        excluir_premium = st.checkbox("Excluir clientes premium")

    st.divider()

    usar_visita = st.toggle("Filtrar última visita", value=True)
    if usar_visita:
        visita_ini = st.date_input("Visita de", dataset["min_visita"].date())
        visita_fim = st.date_input("Visita até", dataset["max_visita"].date())

    usar_compra = st.toggle("Filtrar última compra", value=False)
    if usar_compra and not apenas_sem_compra:
        compra_ini = st.date_input("Compra de", dataset["min_compra"].date())
        compra_fim = st.date_input("Compra até", dataset["max_compra"].date())
    else:
        usar_compra = False

    usar_cadastro = st.toggle("Filtrar cadastro", value=False)
    if usar_cadastro:
        cad_ini = st.date_input("Cadastro de", dataset["min_cadastro"].date())
        cad_fim = st.date_input("Cadastro até", dataset["max_cadastro"].date())

    st.divider()

    aplicar = st.button("🔎 Aplicar filtros", use_container_width=True)

# ==========================================
# QUERY BUILDER
# ==========================================
def build_where():
    cond = []

    if id_busca:
        ids = [i.strip() for i in id_busca.split(",") if i.strip()]
        cond.append(f"member_pk IN ({','.join(f\"'{i}'\" for i in ids)})")

    if cat_sel:
        cond.append(f"categoria IN ({','.join(f\"'{c}'\" for c in cat_sel)})")

    if setor_sel:
        cond.append(f"setor IN ({','.join(f\"'{s}'\" for s in setor_sel)})")

    if usar_visita:
        cond.append(f"data_ultima_visita BETWEEN '{visita_ini}' AND '{visita_fim}'")

    if apenas_sem_compra:
        cond.append("data_ultima_compra IS NULL")
    elif usar_compra:
        cond.append(f"data_ultima_compra BETWEEN '{compra_ini}' AND '{compra_fim}'")

    if usar_cadastro:
        cond.append(f"data_cadastro BETWEEN '{cad_ini}' AND '{cad_fim}'")

    if excluir_premium and dataset["has_premium"]:
        cond.append("(flg_premium_ativo = 'N' OR flg_premium_ativo IS NULL)")

    return " AND ".join(cond) if cond else "1=1"

# ==========================================
# EXECUÇÃO
# ==========================================
if aplicar:
    con = duckdb.connect(database=":memory:")
    where = build_where()

    result = con.execute(f"""
        SELECT
            COUNT(DISTINCT member_pk) clientes,
            COUNT(DISTINCT CASE WHEN data_ultima_compra IS NOT NULL THEN member_pk END) com_compra,
            COUNT(DISTINCT CASE WHEN data_ultima_compra IS NULL THEN member_pk END) sem_compra,
            COUNT(*) linhas
        FROM read_parquet('{dataset["path"]}')
        WHERE {where}
    """).fetchone()

    clientes, com_compra, sem_compra, linhas = result

    st.markdown("### 📊 Resultado")

    c1, c2, c3 = st.columns(3)
    c1.metric("Clientes", f"{clientes:,}")
    c2.metric("Com compra", f"{com_compra:,}")
    c3.metric("Sem compra", f"{sem_compra:,}")

    with st.expander("👁️ Pré-visualização", expanded=True):
        df = con.execute(f"""
            SELECT *
            FROM read_parquet('{dataset["path"]}')
            WHERE {where}
            LIMIT 100
        """).df()

        st.dataframe(df, use_container_width=True, hide_index=True)

    con.close()

# ==========================================
# RODAPÉ
# ==========================================
st.markdown("---")
st.caption(
    f"📦 Base: {dataset['num_rows']:,} registros • Atualizado em {datetime.now().strftime('%d/%m/%Y %H:%M')}"
)
