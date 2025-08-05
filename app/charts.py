import plotly.express as px
import pandas as pd

def create_charts(results: dict):
    """
    Generates Plotly charts from the processed report data.
    `results` is the dictionary of DataFrames returned by `run_report_processing`.
    """
    charts = {}

    # 1. Volume by Book Chart
    try:
        book_results = {
            "A Book": results.get("A Book Result", pd.DataFrame()),
            "B Book": results.get("B Book Result", pd.DataFrame()),
            "Multi Book": results.get("Multi Book Result", pd.DataFrame())
        }
        volumes = {k: df.loc[df['Login'] == 'Summary', 'Total Volume'].iloc[0] for k, df in book_results.items() if not df.empty and 'Total Volume' in df.columns and not df[df['Login'] == 'Summary'].empty}

        if volumes:
            fig_vol = px.bar(
                x=list(volumes.keys()),
                y=list(volumes.values()),
                title="Volume by Book",
                labels={'y': 'Volume (USD)', 'x': 'Book Type'},
                color=list(volumes.keys()),
                color_discrete_sequence=px.colors.qualitative.Pastel
            )
            fig_vol.update_layout(showlegend=False)
            charts['volume_by_book'] = fig_vol.to_html(full_html=False, include_plotlyjs='cdn')
    except Exception as e:
        print(f"Error creating volume_by_book chart: {e}")


    # 2. Broker Profit by Book Chart
    try:
        profits = {k: df.loc[df['Login'] == 'Summary', 'Broker Profit'].iloc[0] for k, df in book_results.items() if not df.empty and 'Broker Profit' in df.columns and not df[df['Login'] == 'Summary'].empty}

        if profits:
            fig_profit = px.pie(
                values=list(profits.values()),
                names=list(profits.keys()),
                title="Broker Profit Distribution by Book",
                color_discrete_sequence=px.colors.sequential.RdBu
            )
            charts['profit_distribution'] = fig_profit.to_html(full_html=False, include_plotlyjs='cdn')
    except Exception as e:
        print(f"Error creating profit_distribution chart: {e}")

    # 3. Client Type Volume Analysis
    try:
        final_calcs = results.get("Final Calculations", pd.DataFrame())
        if not final_calcs.empty:
            calcs = final_calcs.set_index('Source')['Value']
            # Volume in lots * 200,000 = Volume in USD
            chinese_vol = float(calcs.get("Chinese Clients", 0)) * 200000
            vip_vol = float(calcs.get("VIP Clients", 0)) * 200000
            retail_vol = float(calcs.get("Retail Clients", 0)) * 200000

            client_volumes = {
                "Chinese": chinese_vol,
                "VIP": vip_vol,
                "Retail": retail_vol
            }

            if any(v > 0 for v in client_volumes.values()):
                fig_clients = px.bar(
                    x=list(client_volumes.keys()),
                    y=list(client_volumes.values()),
                    title="Client Type Volume Analysis",
                    labels={'y': 'Volume (USD)', 'x': 'Client Type'},
                    color=list(client_volumes.keys())
                )
                charts['client_volume'] = fig_clients.to_html(full_html=False, include_plotlyjs='cdn')
    except Exception as e:
        print(f"Error creating client_volume chart: {e}")


    return charts
