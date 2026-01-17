import streamlit as st
import pandas as pd
import numpy as np
import plotly.express as px

# 1. Configuração da Página (Design UX)
st.set_page_config(
    page_title="Dashboard Pro | Vendas 2024",
    page_icon="🚀",
    layout="wide",
    initial_sidebar_state="expanded"
)

# 2. Gerando Dados Fictícios para o exemplo
@st.cache_data
def gerar_dados():
    np.random.seed(42)
    datas = pd.date_range(start="2024-01-01", end="2024-12-31", freq="D")
    categorias = ['Eletrônicos', 'Moda', 'Home Office', 'Beleza']
    vendedores = ['Ana Paula', 'Bruno Silva', 'Carla Dias', 'Diego Souza']
    
    df = pd.DataFrame({
        'Data': np.random.choice(datas, 500),
        'Categoria': np.random.choice(categorias, 500),
        'Vendedor': np.random.choice(vendedores, 500),
        'Venda': np.random.uniform(100, 5000, 500).round(2),
        'Satisfacao': np.random.randint(1, 6, 500)
    })
    return df.sort_values("Data")

df = gerar_dados()

# 3. Sidebar com UX Refinada
with st.sidebar:
    st.image("https://cdn-icons-png.flaticon.com/512/1055/1055644.png", width=80)
    st.title("Painel de Filtros")
    st.markdown("---")
    
    # Filtro de Data
    data_inicio = st.date_input("Início", df['Data'].min())
    data_fim = st.date_input("Fim", df['Data'].max())
    
    # Filtro de Categoria
    categorias_sel = st.multiselect("Categorias", options=df['Categoria'].unique(), default=df['Categoria'].unique())
    
    st.markdown("---")
    st.info("O dashboard é atualizado automaticamente ao alterar os filtros.")

# Aplicando Filtros
df_filtrado = df[
    (df['Data'].dt.date >= data_inicio) & 
    (df['Data'].dt.date <= data_fim) &
    (df['Categoria'].isin(categorias_sel))
]

# 4. Cabeçalho do Painel
st.title("🚀 Dashboard Executivo de Vendas")
st.markdown(f"Exibindo dados de **{data_inicio}** até **{data_fim}**")

# 5. Métricas (KPIs) com estilo UX
m1, m2, m3, m4 = st.columns(4)

total_vendas = df_filtrado['Venda'].sum()
ticket_medio = df_filtrado['Venda'].mean()
qtd_pedidos = len(df_filtrado)
media_sat = df_filtrado['Satisfacao'].mean()

m1.metric("Faturamento Total", f"R$ {total_vendas:,.2f}", delta=f"{len(df_filtrado)} vendas")
m2.metric("Ticket Médio", f"R$ {ticket_medio:,.2f}")
m3.metric("Qtd. Pedidos", qtd_pedidos)
m4.metric("Satisfação Média", f"{media_sat:.1f} ⭐")

st.markdown("---")

# 6. Gráficos (Visualização de Dados)
col_esq, col_dir = st.columns([2, 1])

with col_esq:
    st.subheader("Evolução de Vendas no Tempo")
    # Agrupando por data para o gráfico de linha
    df_tempo = df_filtrado.groupby('Data')['Venda'].sum().reset_index()
    fig_linha = px.line(df_tempo, x='Data', y='Venda', template="plotly_white", color_discrete_sequence=['#00CC96'])
    st.plotly_chart(fig_linha, use_container_width=True)

with col_dir:
    st.subheader("Vendas por Categoria")
    fig_pizza = px.pie(df_filtrado, values='Venda', names='Categoria', hole=0.4)
    st.plotly_chart(fig_pizza, use_container_width=True)

# 7. Tabela e Exportação
with st.expander("🔎 Visualizar base completa e exportar"):
    st.dataframe(df_filtrado, use_container_width=True)
    
    csv = df_filtrado.to_csv(index=False).encode('utf-8')
    st.download_button(
        label="📥 Baixar Dados Filtrados (CSV)",
        data=csv,
        file_name='vendas_processadas.csv',
        mime='text/csv',
    )

# 8. Rodapé Estilizado
st.markdown(
    """
    <style>
    .footer {
        position: fixed;
        left: 0;
        bottom: 0;
        width: 100%;
        background-color: #f0f2f6;
        color: #31333F;
        text-align: center;
        padding: 10px;
        font-size: 12px;
    }
    </style>
    <div class="footer">Dashboard desenvolvido com Streamlit | 2024</div>
    """,
    unsafe_allow_html=True
)
