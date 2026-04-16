import requests
import pandas as pd
from datetime import datetime, timedelta
from config import *

# =========================
# ZABBIX API
# =========================
def zabbix_api(method, params, auth=None):
    payload = {
        "jsonrpc": "2.0",
        "method": method,
        "params": params,
        "id": 1,
        "auth": auth
    }
    response = requests.post(ZABBIX_URL, json=payload)
    response.raise_for_status()
    data = response.json()
    if "error" in data:
        print("❌ ERRO API:", data["error"])
        raise Exception(data["error"])
    return response.json()["result"]

# AUTENTICAÇÃO VIA TOKEN ZABBIX
def zabbix_login():
    return ZABBIX_TOKEN

# =========================
# PERÍODO MÊS ANTERIOR
# =========================
def get_custom_period():

    start = datetime.strptime(START_DATE, "%Y-%m-%d")
    end = datetime.strptime(END_DATE, "%Y-%m-%d")
    end = end.replace(hour=23, minute=59, second=59)
    return int(start.timestamp()), int(end.timestamp()), start, end

# ================================
# BUSCA POR GRUPOS DE HOSTS E HOSTS
# ================================
def get_hosts_by_groups(auth, group_names):

    groups = zabbix_api("hostgroup.get"), {
    "filter": {"name": group_names},
    "output": ["groupid", "name"]
    }, 
    groupids = [g["groupid"] for g in groups]
    print("Buscando grupos:", groupids)
    print("Grupos encontrados:", groups)
    return zabbix_api("host.get", {
        "groupids": groupids,
        "filter": {"status": 0},
        "output": ["hostid", "name"]
    }, auth)
   
# =========================
# TRIGGERS
# =========================
# NOVA FUNÇÃO QUE BUSCA TRIGGERS DE UMA VEZ (POR HOST GROUP)
def get_triggers_bulk(auth, hostids):

    return zabbix_api("trigger.get", {
        "hostids": hostids,
        "output": ["triggerid", "description"],
        "selectHosts": ["host","name"], 
        "filter": {"status": 0},
        "expandDescription": True
    }, auth)

# =========================
# EVENTOS
# =========================
# NOVA FUNÇÃO QUE BUSCA EVENTO EM LOTE (BULK)
def get_events_bulk(auth, trigger_ids, time_from, time_till):
    return zabbix_api("event.get", {
        "objectids": trigger_ids,
        "time_from": time_from,
        "time_till": time_till,
        "value": 1,
        "output": ["eventid", "objectid", "clock", "r_eventid"]
    }, auth)

# =========================
# EVENTO DE RECOVERY
# =========================
# NOVA FUNÇÃO EVENTO DE RECOVERY (ELIMINA CHAMADAS DE RECOVERY)
def get_recovery_bulk(auth, recovery_ids):

    if not recovery_ids:
        return {}
    recoveries = zabbix_api("event.get", {
        "eventids": recovery_ids,
        "output": ["eventid", "clock"]
    }, auth)
    return {r["eventid"]: int(r["clock"]) for r in recoveries}

# =========================
# CÁLCULO DOWNTIME
# =========================
# NOVO CALCULO DOWNTIME (SEM CHAMADA API)
def calculate_downtime_optimized(events, recovery_map):
    downtime = 0
    for event in events:
        start = int(event["clock"])
        if event["r_eventid"] != "0":
            end = recovery_map.get(event["r_eventid"])
            if end:
                downtime += (end - start)
    return downtime

#  format downtime
def format_downtime(minutes):
    minutes = int(minutes)
    hours = minutes // 60
    mins = minutes % 60

    if hours > 0 and mins > 0:
        return f"{hours}h {mins}min"
    elif hours > 0:
        return f"{hours}h"
    else:
        return f"{mins}min"

# =========================
# RELATÓRIO
# =========================
def main():

    auth = zabbix_login()
    time_from, time_till, start_dt, end_dt = get_custom_period()
    total_period = time_till - time_from
    writer = pd.ExcelWriter(OUTPUT_FILE, engine='xlsxwriter')
    workbook = writer.book
    summary_data = []

    for categoria, grupos in HOST_GROUPS.items():

        print(f"\nProcessando categoria: {categoria}")
        hosts = get_hosts_by_groups(auth, grupos)
        print(f"Hosts encontrados em {categoria}: {len(hosts)}")

        rows = []

        # LOOP OTIMIZADO
        hostids = [h["hostid"] for h in hosts]
        #print("Host:", hostids["name"])
        triggers = get_triggers_bulk(auth, hostids)
        trigger_map = {t["triggerid"]: t for t in triggers}
        trigger_ids = list(trigger_map.keys())
        events = get_events_bulk(auth, trigger_ids, time_from, time_till)

        #AGRUPAR EVENTOS POR TRIGGER
        from collections import defaultdict
        events_by_trigger = defaultdict(list)
        for event in events:
            events_by_trigger[event["objectid"]].append(event)

        # PEGAR RECOBERY IDS
        recovery_ids = [e["r_eventid"] for e in events if e["r_eventid"] != "0"]
        recovery_map = get_recovery_bulk(auth, recovery_ids)

        for trigger_id, trigger in trigger_map.items():
            trigger_events = events_by_trigger.get(trigger_id, [])
            incidents = len(trigger_events)
                #if incidents == 0:
                #  continue
            print(f"Trigger: {trigger['description']} | Eventos: {incidents}")

            downtime = calculate_downtime_optimized(trigger_events, recovery_map)
            downtime_percent = (downtime / total_period) * 100
            availability = 100 - downtime_percent
            downtime_minutes = downtime / 60
            host_name = trigger["hosts"][0]["name"] if trigger["hosts"] else "UNKNOWN"

            rows.append({
                    "Host": host_name,
                    "Trigger": trigger["description"],
                    "Incidentes": incidents,
                    #"Downtime (min)": round(downtime_minutes, 2),
                    "Downtime": format_downtime(downtime_minutes),
                    "Downtime (%)": round(downtime_percent, 4),
                    "Disponibilidade (%)": round(availability, 4)            
                })

        print(f"Total de linhas para {categoria}:", len(rows))
        df = pd.DataFrame(rows)
        if df.empty:
            print(f"⚠️ Sem dados para categoria: {categoria}")
            continue
        # =========================
        # ORDENAÇÃO
        # =========================
        df = df.sort_values(by=["Host", "Trigger"])
        # =========================
        # MÉDIAS
        # =========================
        media_downtime = df["Downtime (%)"].mean()
        media_disp = df["Disponibilidade (%)"].mean()

        # garantir tipo numérico antes de qualquer cálculo
        df["Incidentes"] = pd.to_numeric(df["Incidentes"], errors="coerce")
        df["Disponibilidade (%)"] = pd.to_numeric(df["Disponibilidade (%)"], errors="coerce")
        df["Downtime (%)"] = pd.to_numeric(df["Downtime (%)"], errors="coerce")

        # 🔥 calcula resumo ANTES da linha MÉDIA
        summary_data.append({
        "Categoria": categoria,
        "Disponibilidade Média (%)": round(df["Disponibilidade (%)"].mean(), 4),
        "Downtime Médio (%)": round(df["Downtime (%)"].mean(), 4),
        "Total Incidentes": int(df["Incidentes"].sum())
        })

        # Adiciona linha de média
        df.loc[len(df)] = [
            "",
            "MÉDIA",
            "",
            "",
            round(media_downtime, 4),
            round(media_disp, 4)]

        # =========================
        # EXPORTAÇÃO
        # =========================
        df.to_excel(writer, sheet_name=categoria, index=False)
        worksheet = writer.sheets[categoria]
        workbook = writer.book

        # =========================
        # FORMATAÇÃO VISUAL
        # =========================
        # 🔹 Negrito na linha de média
        bold_format = workbook.add_format({'bold': True})
        last_row = len(df)  # índice da última linha (excel começa em 1 por causa do header)
        worksheet.set_row(last_row, None, bold_format)

        # 🔹 Auto ajuste das colunas
        for i, col in enumerate(df.columns):
            column_len = max(
                df[col].astype(str).map(len).max(),
                len(col)
            )
            worksheet.set_column(i, i, column_len + 2)

        # 🔹 DOWNTIME (%) → vermelho se > 0
        worksheet.conditional_format(1, 4, last_row, 4, {
        'type': 'cell',
        'criteria': '>',
        'value': 0,
        'format': workbook.add_format({'bg_color': '#FFC7CE'})
        })
        # 🔹 DISPONIBILIDADE (%) → verde (>=99)
        worksheet.conditional_format(1, 5, last_row, 5, {
          'type': 'cell',
          'criteria': '>=',
          'value': 99,
          'format': workbook.add_format({'bg_color': '#C6EFCE'})
        })
        # 🔹 DISPONIBILIDADE média → amarelo
        worksheet.conditional_format(1, 5, last_row, 5, {
          'type': 'cell',
          'criteria': 'between',
          'minimum': 95,
          'maximum': 98.9999,
          'format': workbook.add_format({'bg_color': '#FFEB9C'})
        })
        # 🔹 DISPONIBILIDADE baixa → vermelho
        worksheet.conditional_format(1, 5, last_row, 5, {
            'type': 'cell',
            'criteria': '<',
            'value': 95,
            'format': workbook.add_format({'bg_color': '#FFC7CE'})
        })
    # =========================
    # ABA RESUMO (FINAL)
    # =========================
    summary_df = pd.DataFrame(summary_data)
    if not summary_df.empty:

    # ordenar por pior disponibilidade
    #    summary_df = summary_df.sort_values(by="Disponibilidade Média (%)")
    # média geral
        media_disp_geral = summary_df["Disponibilidade Média (%)"].mean()
        media_downtime_geral = summary_df["Downtime Médio (%)"].mean()
        total_incidentes = summary_df["Total Incidentes"].sum()
    # adicionar linha TOTAL
        summary_df.loc[len(summary_df)] = {
        "Categoria": "TOTAL GERAL",
        "Disponibilidade Média (%)": round(media_disp_geral, 4),
        "Downtime Médio (%)": round(media_downtime_geral, 4),
        "Total Incidentes": int(total_incidentes)
        }
        summary_df.to_excel(writer, sheet_name="RESUMO", index=False)
        worksheet = writer.sheets["RESUMO"]

        # auto ajuste
        for i, col in enumerate(summary_df.columns):
            column_len = max(
                summary_df[col].astype(str).map(len).max(),
            len(col))
            worksheet.set_column(i, i, column_len + 2)
        # formatação condicional (disponibilidade)
             # 🔹 DISPONIBILIDADE (%) → verde (>=99)
            worksheet.conditional_format(1, 2, last_row, 2, {
            'type': 'cell',
            'criteria': '>=',
            'value': 99,
            'format': workbook.add_format({'bg_color': '#C6EFCE'})
            })
            # 🔹 DISPONIBILIDADE média → amarelo
            worksheet.conditional_format(1, 2, last_row, 2, {
            'type': 'cell',
            'criteria': 'between',
            'minimum': 95,
            'maximum': 98.9999,
            'format': workbook.add_format({'bg_color': '#FFEB9C'})
            })
        # 🔹 DISPONIBILIDADE baixa → vermelho
            worksheet.conditional_format(1, 2, last_row, 2, {
            'type': 'cell',
            'criteria': '<',
            'value': 95,
            'format': workbook.add_format({'bg_color': '#FFC7CE'})
            })

    writer.close()
    print("\nRelatório gerado:", OUTPUT_FILE)
if __name__ == "__main__": 
    main()