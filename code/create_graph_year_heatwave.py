import pandas as pd
import plotly.graph_objects as go

# Dados CSV (você pode substituir isso por leitura de um arquivo CSV real)
from io import StringIO

df = pd.read_csv("Heatwaves/dados/tmax_ref.csv")
df["year"] = pd.to_datetime(df["time"]).dt.year
df = df.groupby("year")[["greater", "heatwave", "cummulative"]].sum().reset_index()

# Criar gráfico interativo
fig = go.Figure()

# Temperatura máxima observada
fig.add_trace(go.Scatter(x=df["year"], y=df["heatwave"], mode='lines+markers', name="heatwave"))

# Layout
fig.update_layout(
    title="Heatwave by year",
    xaxis_title="Year",
    yaxis_title="Temperature (°C)",
    template="plotly_white"
)

# Exportar como HTML
fig.write_html("./graph/heatwave.html")
print("✅ Gráfico salvo como 'grafico_temperaturas.html'")

