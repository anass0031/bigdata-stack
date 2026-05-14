import streamlit as st
import subprocess
import re
import time

# ==========================================
# 1. CONFIGURATION DE LA PAGE
# ==========================================
st.set_page_config(page_title="Docker Monitor Temps Réel", layout="wide")

st.title("Moniteur Docker Temps Réel")
st.markdown("Surveillance de l'état des services et affichage chronologique des exécutions.")
st.markdown("---")

# ==========================================
# 2. CONFIGURATION DES SERVICES
# ==========================================
SERVICES = [
    "postgres", "minio", "zookeeper", "kafka", 
    "spark-master", "spark-worker", "airflow", 
    "statsd-exporter", "cnn-streamer", 
    "spark-streaming-job", "metabase", 
    "prometheus", "grafana"
]

LIGNES_A_LIRE = 500

# ==========================================
# 3. FONCTIONS OUTILS
# ==========================================

def get_container_status(container_name):
    """Interroge Docker pour connaître l'état exact du conteneur en temps réel."""
    try:
        result = subprocess.run(
            ["docker", "inspect", "-f", "{{.State.Status}}", container_name],
            stdout=subprocess.PIPE, text=True, check=False
        )
        status = result.stdout.strip().lower()
        
        if status == "running": return "[En ligne]"
        elif status == "exited": return "[Arrêté]"
        elif status == "restarting": return "[Redémarrage]"
        elif status == "": return "[Introuvable]"
        else: return f"[{status}]"
    except Exception:
        return "[Erreur de vérification]"

def clean_ansi_codes(text):
    """Nettoie les codes de couleur du terminal (ANSI escape sequences)."""
    ansi_escape = re.compile(r'(?:\x1B[@-_]|[\x80-\x9F])[0-?]*[ -/]*[@-~]')
    return ansi_escape.sub('', text)

def categorize_line(ligne):
    """Classe les lignes. Ce qui n'est ni erreur, ni warning, ni info va dans PRINT."""
    ligne_lower = ligne.lower()
    if re.search(r"(\[error\]|\berror\b:? |\bexception\b|\bfatal\b|\btraceback\b)", ligne_lower):
        return "ERROR"
    if re.search(r"(\[warn(ing)?\]|\bwarn(ing)?\b:? |\btimeout\b|\bretry\b|\bdeprecated\b|\bdeprecation\b)", ligne_lower):
        return "WARN"
    if re.search(r"(\[info\]|\binfo\b:? |\blog\b:? |\bstart\b|success|\bready\b|\bconnected\b|\bdebug\b)", ligne_lower):
        return "INFO"
    return "PRINT"

def get_categorized_logs(container_name):
    """Récupère les logs avec l'ordre CHRONOLOGIQUE exact garanti."""
    erreurs, warnings, infos, prints = [], [], [], []
    
    try:
        # CORRECTION ICI: stderr=subprocess.STDOUT fusionne les flux pour garder l'ordre CHRONOLOGIQUE
        result = subprocess.run(
            ["docker", "logs", "--tail", str(LIGNES_A_LIRE), container_name],
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT, 
            text=True, 
            encoding="utf-8", 
            errors="replace",
            check=False
        )
        
        tous_les_logs = result.stdout
        
        for ligne in tous_les_logs.split('\n'):
            if not ligne.strip():
                continue
                
            ligne_propre = clean_ansi_codes(ligne)
            cat = categorize_line(ligne_propre)
            
            if cat == "ERROR":
                erreurs.append(ligne_propre)
            elif cat == "WARN":
                warnings.append(ligne_propre)
            elif cat == "INFO":
                infos.append(ligne_propre)
            else:
                prints.append(ligne_propre)
                
        # Les prints gardent maintenant l'intégralité de leur ordre naturel
        return erreurs, warnings, infos[-100:], prints, None
    except Exception as e:
        return None, None, None, None, f"Erreur de lecture système : {str(e)}"

# ==========================================
# 4. INTERFACE UTILISATEUR
# ==========================================

col1, col2 = st.columns([3, 1])
with col1:
    st.markdown(f"Analyse des **{LIGNES_A_LIRE} dernières lignes** de logs.")
with col2:
    auto_refresh = st.checkbox("Temps Réel (Auto-refresh 5s)")
    if st.button("Rafraîchir maintenant"):
        st.rerun()

for service in SERVICES:
    statut_temps_reel = get_container_status(service)
    erreurs, warnings, infos, prints, message_systeme = get_categorized_logs(service)
    
    a_des_erreurs_ou_prints = bool(erreurs) or (service in ["cnn-streamer", "spark-streaming-job", "airflow"] and bool(prints))
    
    with st.expander(f"{statut_temps_reel} | {service}", expanded=a_des_erreurs_ou_prints):
        
        if statut_temps_reel == "[Introuvable]":
            st.error("Le conteneur est introuvable ou n'a pas encore été créé.", icon=None)
            continue
            
        if message_systeme:
            st.error(message_systeme, icon=None)
            continue

        tab_err, tab_warn, tab_info, tab_print = st.tabs([
            f"Erreurs ({len(erreurs)})", 
            f"Warnings ({len(warnings)})", 
            f"Infos ({len(infos)})",
            f"Prints Scripts ({len(prints)})"
        ])
        
        with tab_err:
            if erreurs:
                st.code('\n'.join(erreurs), language="bash")
            else:
                st.success("Aucune erreur détectée.", icon=None)
                
        with tab_warn:
            if warnings:
                st.code('\n'.join(warnings), language="bash")
            else:
                st.info("Aucun avertissement.", icon=None)
                
        with tab_info:
            if infos:
                st.code('\n'.join(infos), language="bash")
            else:
                st.info("Aucune information système.", icon=None)
                
        with tab_print:
            if prints:
                st.code('\n'.join(prints), language="bash")
            else:
                st.info("Aucun résultat d'exécution de script détecté.", icon=None)

# ==========================================
# 5. BOUCLE TEMPS RÉEL (AUTO-REFRESH)
# ==========================================
if auto_refresh:
    time.sleep(5)
    st.rerun()