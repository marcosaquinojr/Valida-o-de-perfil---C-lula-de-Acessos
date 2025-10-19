import os
import sqlite3
import pandas as pd
import io
import re
from datetime import datetime
from flask import Flask, render_template, jsonify, request, redirect, url_for, send_from_directory, send_file, flash
from werkzeug.utils import secure_filename
from rapidfuzz import process, fuzz
import traceback

# --- Configuração ---
app = Flask(__name__)
app.secret_key = os.urandom(24)

BASE_DIR = os.path.abspath(os.path.dirname(__file__))
UPLOAD_FOLDER = os.path.join(BASE_DIR, 'uploads')
DATABASE_PATH = os.path.join(BASE_DIR, 'sistema.db')

app.config.update(
    UPLOAD_FOLDER=UPLOAD_FOLDER,
    DATABASE=DATABASE_PATH
)

LIMIAR_ALERTA = 0.90 
MODULOS_PADRAO = [
    'TOTVS Educacional', 'TOTVS Folha de Pagamento', 'TOTVS Gestão Contábil',
    'TOTVS Gestão de Estoque, Compras e Faturamento', 'TOTVS Gestão de Pessoas',
    'TOTVS Gestão Financeira', 'TOTVS Gestão Fiscal', 'TOTVS Gestão Patrimonial',
    'TOTVS Inteligência de Negócios'
]

# --- Funções Auxiliares e Setup ---
def get_db_connection():
    conn = sqlite3.connect(app.config['DATABASE'])
    conn.row_factory = sqlite3.Row
    return conn

def init_db():
    conn = get_db_connection()
    conn.execute('''
        CREATE TABLE IF NOT EXISTS uploads_historico (
            id INTEGER PRIMARY KEY AUTOINCREMENT, nome_arquivo_original TEXT NOT NULL,
            nome_arquivo_salvo TEXT NOT NULL, timestamp DATETIME NOT NULL, status TEXT NOT NULL 
        )
    ''')
    conn.execute('CREATE TABLE IF NOT EXISTS dados_var ("ID Funcionalidade" TEXT, "Funcionalidade" TEXT, "ID Módulo" TEXT, "Módulo" TEXT)')
    conn.commit()
    conn.close()

def setup():
    os.makedirs(UPLOAD_FOLDER, exist_ok=True)
    init_db()

setup()

def allowed_file(filename, allowed_extensions={'xlsx', 'xls'}):
    return '.' in filename and filename.rsplit('.', 1)[1].lower() in allowed_extensions

# --- LÓGICA DO ANALISADOR DE RISCOS SoD (Atualizada) ---

def encontrar_linha_titulo(df_bruto, padrao_titulo):
    for index, row in df_bruto.iterrows():
        cell_value = str(row.iloc[0])
        if padrao_titulo in cell_value:
            return index
    return None

def consolidar_colunas(df_bruto_slice, col_inicio, col_fim):
    col_fim_real = min(col_fim, len(df_bruto_slice.columns))
    return df_bruto_slice.iloc[:, col_inicio:col_fim_real].fillna('').astype(str).agg(''.join, axis=1)

def analisar_riscos_excel(caminho_arquivo, cenario):
    try:
        df_bruto = pd.read_excel(caminho_arquivo, header=None, sheet_name=0)
    except Exception as e:
        return {'status': 'error', 'message': f"Erro ao ler o arquivo Excel: {e}", 'cenario': cenario}

    linha_titulo_t1 = encontrar_linha_titulo(df_bruto, "Relatório da Análise de ticket do perfil")
    linha_titulo_t2 = encontrar_linha_titulo(df_bruto, "Riscos SoD para perfil")

    if (cenario == 'manutencao' and linha_titulo_t1 is None) or linha_titulo_t2 is None:
        return {'status': 'error', 'message': "Não foi possível encontrar os títulos das tabelas necessárias para este cenário.", 'cenario': cenario}

    perfil_analisado = "Não identificado"
    if linha_titulo_t1 is not None:
        try:
            titulo_completo_t1 = df_bruto.iloc[linha_titulo_t1, 0]
            match = re.search(r"Relatório da Análise de ticket do perfil\s*(.*)", titulo_completo_t1, re.IGNORECASE)
            if match:
                perfil_analisado = match.group(1).strip()
        except Exception:
            pass
    
    try:
        dados_t2_bruto = df_bruto.iloc[linha_titulo_t2 + 2 :]
        riscos_data = {
            'ID Risco': consolidar_colunas(dados_t2_bruto, 0, 2),
            'Descrição Risco': consolidar_colunas(dados_t2_bruto, 2, 6),
            'Criticidade': consolidar_colunas(dados_t2_bruto, 6, 8),
            'Aprovador': consolidar_colunas(dados_t2_bruto, 8, 10),
            'Sistema': consolidar_colunas(dados_t2_bruto, 10, 12),
            'Módulo': consolidar_colunas(dados_t2_bruto, 12, 14),
            'Atividade': consolidar_colunas(dados_t2_bruto, 14, 16),
            'Funcionalidade': consolidar_colunas(dados_t2_bruto, 16, 18),
            'Atividade2': consolidar_colunas(dados_t2_bruto, 18, 20),
            'Funcionalidade 2': consolidar_colunas(dados_t2_bruto, 20, 22)
        }
        df_riscos = pd.DataFrame(riscos_data).dropna(how='all')
    except Exception as e:
        return {'status': 'error', 'message': f"Erro ao reconstruir a Tabela 2 (Riscos). Detalhes: {e}", 'perfil': perfil_analisado, 'cenario': cenario}

    if cenario == 'criacao':
        if df_riscos.empty:
            return {'status': 'no_risks', 'message': "Nenhum risco encontrado na Tabela de Riscos SoD.", 'perfil': perfil_analisado, 'cenario': cenario}
        else:
            dados_finais = df_riscos.to_dict('records')
            dados_agrupados = {'Riscos Identificados na Base': dados_finais}
            return {'status': 'success', 'message': "Atenção! Foram encontrados os seguintes riscos na Tabela de Riscos SoD:", 'data': dados_agrupados, 'perfil': perfil_analisado, 'cenario': cenario}

    elif cenario == 'manutencao':
        try:
            dados_t1_bruto = df_bruto.iloc[linha_titulo_t1 + 2 : linha_titulo_t2 - 1]
            relatorio_data = {
                'Perfil': consolidar_colunas(dados_t1_bruto, 0, 2),
                'Sistema': consolidar_colunas(dados_t1_bruto, 2, 4),
                'Funcionalidade': consolidar_colunas(dados_t1_bruto, 4, 8),
                'Status': consolidar_colunas(dados_t1_bruto, 8, 10)
            }
            df_relatorio = pd.DataFrame(relatorio_data).dropna(how='all')
        except Exception as e:
            return {'status': 'error', 'message': f"Erro ao reconstruir a Tabela 1 (Relatório). Detalhes: {e}", 'perfil': perfil_analisado, 'cenario': cenario}
        
        df_adicionadas = df_relatorio[df_relatorio['Status'].str.strip() == 'Adicionado']
        if df_adicionadas.empty:
            return {'status': 'no_risks', 'message': "Nenhuma funcionalidade com status 'Adicionado' foi encontrada para análise.", 'perfil': perfil_analisado, 'cenario': cenario}
        
        funcionalidades_para_verificar = df_adicionadas['Funcionalidade'].dropna().unique()
        riscos_encontrados = df_riscos[
            df_riscos['Funcionalidade'].isin(funcionalidades_para_verificar) |
            df_riscos['Funcionalidade 2'].isin(funcionalidades_para_verificar)
        ].copy()

        if riscos_encontrados.empty:
            return {'status': 'no_risks', 'message': "As funcionalidades adicionadas não apresentaram riscos SoD.", 'perfil': perfil_analisado, 'cenario': cenario}
        else:
            resultado_detalhado = pd.merge(df_adicionadas, riscos_encontrados, left_on='Funcionalidade', right_on='Funcionalidade', how='inner')
            resultado_detalhado.rename(columns={'Sistema_x': 'Sistema'}, inplace=True)
            modulos_encontrados = resultado_detalhado['Módulo'].dropna().unique().tolist()
            modulos_str = ", ".join(modulos_encontrados)
            
            dados_agrupados = {}
            for index, row in resultado_detalhado.iterrows():
                func = row['Funcionalidade']
                if func not in dados_agrupados: dados_agrupados[func] = []
                dados_agrupados[func].append({
                    'ID Risco': row['ID Risco'], 'Descrição Risco': row['Descrição Risco'], 'Criticidade': row['Criticidade'],
                    'Perfil': row['Perfil'], 'Sistema': row['Sistema']
                })
            
            return {'status': 'success', 'message': "Atenção! Foram encontrados os seguintes riscos para as funcionalidades adicionadas:", 'data': dados_agrupados, 'perfil': perfil_analisado, 'modulos': modulos_str, 'cenario': cenario}

# --- ROTAS PRINCIPAIS ---
@app.route('/')
def home():
    return render_template('home.html', active_app='home')

# --- ROTAS DO VALIDADOR DE PERFIS (sem alterações) ---
@app.route('/validator')
def validator():
    conn = get_db_connection()
    historico_bruto = conn.execute('SELECT * FROM uploads_historico ORDER BY timestamp DESC').fetchall()
    historico_formatado = [{'id': row['id'], 'nome_arquivo_original': row['nome_arquivo_original'], 'timestamp_formatado': datetime.fromisoformat(row['timestamp']).strftime('%d/%m/%Y às %H:%M') if row['timestamp'] else '', 'status': row['status']} for row in historico_bruto]
    modulos_disponiveis = []
    is_var_active = False
    try:
        if conn.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name='dados_var'").fetchone() and conn.execute('SELECT 1 FROM dados_var').fetchone():
            is_var_active = True
            modulos_df = pd.read_sql_query('SELECT DISTINCT "Módulo" FROM dados_var', conn)
            if not modulos_df.empty:
                modulos_disponiveis = modulos_df["Módulo"].dropna().sort_values().tolist()
    except Exception as e:
        app.logger.warning(f"Não foi possível carregar módulos do DB. Erro: {e}", exc_info=True)
    if not modulos_disponiveis:
        modulos_disponiveis = MODULOS_PADRAO
    conn.close()
    return render_template('validator/index.html', historico=historico_formatado, modulos=modulos_disponiveis, is_var_active=is_var_active, active_app='validator')

@app.route('/upload_var', methods=['POST'])
def upload_var():
    if 'file' not in request.files or not request.files['file'].filename:
        flash('Nenhum arquivo selecionado.', 'warning')
    else:
        file = request.files['file']
        if allowed_file(file.filename):
            original_filename = secure_filename(file.filename)
            try:
                df = pd.read_excel(file)
                if not all(col in df.columns for col in ["id", "funcionalidade", "modulo id", "modulo"]):
                    flash(f"Arquivo inválido! Colunas esperadas não encontradas.", 'danger')
                else:
                    timestamp = datetime.now()
                    saved_filename = f"{timestamp.strftime('%Y-%m-%d_%H-%M-%S')}_{original_filename}"
                    filepath = os.path.join(app.config['UPLOAD_FOLDER'], saved_filename)
                    file.seek(0)
                    file.save(filepath)
                    conn = get_db_connection()
                    conn.execute('INSERT INTO uploads_historico (nome_arquivo_original, nome_arquivo_salvo, timestamp, status) VALUES (?, ?, ?, "Válido")', (original_filename, saved_filename, timestamp))
                    conn.commit()
                    conn.close()
                    flash("Arquivo enviado e validado com sucesso!", 'success')
            except Exception as e:
                flash(f"Erro ao ler o arquivo Excel: {e}", 'danger')
        else:
            flash('Tipo de arquivo inválido. Por favor, envie um arquivo .xlsx.', 'danger')
    return redirect(url_for('validator', open_modal='config'))

@app.route('/ativar_var/<int:upload_id>')
def ativar_var(upload_id):
    conn = get_db_connection()
    upload = conn.execute('SELECT * FROM uploads_historico WHERE id = ? AND status IN ("Válido", "Arquivado")', (upload_id,)).fetchone()
    if upload:
        try:
            filepath = os.path.join(app.config['UPLOAD_FOLDER'], upload['nome_arquivo_salvo'])
            df = pd.read_excel(filepath)
            mapa_de_nomes = { "id": "ID Funcionalidade", "funcionalidade": "Funcionalidade", "modulo id": "ID Módulo", "modulo": "Módulo" }
            df_renomeado = df[list(mapa_de_nomes.keys())].rename(columns=mapa_de_nomes)
            df_renomeado.to_sql('dados_var', conn, if_exists='replace', index=False)
            conn.execute('UPDATE uploads_historico SET status = "Arquivado" WHERE status = "Ativo"')
            conn.execute('UPDATE uploads_historico SET status = "Ativo" WHERE id = ?', (upload_id,))
            conn.commit()
            flash(f"Planilha '{upload['nome_arquivo_original']}' ativada com sucesso!", 'success')
        except Exception as e:
            app.logger.error(f"Falha ao ativar a planilha ID {upload_id}", exc_info=True)
            flash("ERRO AO ATIVAR: Falha ao processar o arquivo.", 'danger')
            conn.execute('UPDATE uploads_historico SET status = "Inválido" WHERE id = ?', (upload_id,)).commit()
    else:
        flash("Arquivo para ativação não encontrado ou inválido.", 'danger')
    conn.close()
    return redirect(url_for('validator', open_modal='config'))

@app.route('/comparar', methods=['POST'])
def comparar():
    try:
        conn = get_db_connection()
        if not conn.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name='dados_var'").fetchone() or not conn.execute('SELECT 1 FROM dados_var').fetchone():
            return jsonify({'erro': 'Nenhuma Planilha VAR está ativa. Por favor, vá para as "Configurações" e ative uma.'}), 400
    except Exception as e:
        return jsonify({'erro': 'Ocorreu um erro ao verificar a base de dados.'}), 500

    if 'arquivo_analise' not in request.files: return jsonify({'erro': 'Nenhum arquivo de análise enviado.'}), 400
    
    try:
        arquivo_usuario = request.files['arquivo_analise']
        modulo_selecionado = request.form.get('modulo')
        df_usuario = pd.read_excel(arquivo_usuario, usecols=[0])
        if df_usuario.empty or df_usuario.columns[0].lower().strip() not in ['funcionalidade', 'funcionalidades']:
            return jsonify({'erro': f"Arquivo de análise inválido! O cabeçalho da primeira coluna ('{df_usuario.columns[0]}') deve ser 'Funcionalidade'."}), 400
        
        normalize = lambda s: str(s).lower().strip()
        lista_func_usuario = [normalize(func) for func in df_usuario.iloc[:, 0].dropna()]
        if not lista_func_usuario: return jsonify({'mensagem': 'Nenhuma funcionalidade para analisar na planilha enviada.'})
        
        conn = get_db_connection()
        df_var = pd.read_sql_query('SELECT "ID Funcionalidade", "Funcionalidade" FROM dados_var WHERE "Módulo" = ?', conn, params=(modulo_selecionado,))
        conn.close()
        
        df_var['Funcionalidade_norm'] = df_var['Funcionalidade'].apply(normalize)
        func_var_map = {row['Funcionalidade_norm']: {'id': row['ID Funcionalidade'], 'nome_original': row['Funcionalidade']} for _, row in df_var.iterrows()}
        lista_func_var_norm = df_var['Funcionalidade_norm'].tolist()
    except Exception as e:
        return jsonify({'erro': 'Falha interna no servidor ao processar os arquivos.'}), 500

    resultados_finais = []
    for idx, func_usuario_norm in enumerate(lista_func_usuario):
        item = {'id': idx, 'Funcionalidade Analisada': func_usuario_norm, 'Status': 'Divergente', 'ID Encontrado': '', 'ID Sugerido': '', 'Sugestão Similar (VAR)': '', 'Similaridade (%)': 0.0}
        if func_usuario_norm in func_var_map:
            match = func_var_map[func_usuario_norm]
            item.update({'Status': 'Encontrado', 'ID Encontrado': str(match['id']), 'Sugestão Similar (VAR)': match['nome_original'], 'Similaridade (%)': 100.0})
        else:
            best_match = process.extractOne(func_usuario_norm, lista_func_var_norm, scorer=fuzz.WRatio, score_cutoff=80)
            if best_match:
                sugestao_norm, score, _ = best_match
                match = func_var_map[sugestao_norm]
                item.update({'Status': 'Divergente com Sugestão', 'ID Sugerido': str(match['id']), 'Sugestão Similar (VAR)': match['nome_original'], 'Similaridade (%)': round(score, 2)})
        resultados_finais.append(item)
    
    sort_order = {'Divergente com Sugestão': 0, 'Divergente': 1, 'Encontrado': 2}
    resultados_finais.sort(key=lambda item: sort_order.get(item['Status'], 99))
    
    json_response = {}
    divergentes_count = sum(1 for r in resultados_finais if r['Status'] != 'Encontrado')

    if resultados_finais:
        if divergentes_count == 0:
            json_response['mensagem_status'] = {"texto": "Análise concluída sem divergências! Todas as funcionalidades foram encontradas com sucesso.", "tipo": "sucesso"}
        elif (divergentes_count / len(resultados_finais)) >= LIMIAR_ALERTA:
            json_response['mensagem_status'] = {"texto": "Atenção: Alta taxa de divergência. Verifique se o módulo selecionado está correto antes de prosseguir.", "tipo": "alerta"}
        else:
            json_response['mensagem_status'] = {"texto": f"Análise com ressalvas. Foram encontradas {divergentes_count} divergências que podem requerer atenção.", "tipo": "ressalva"}

    json_response.update({'resultados': resultados_finais})
    return jsonify(json_response)

@app.route('/gerar_importacao', methods=['POST'])
def gerar_importacao():
    data = request.get_json()
    resultados_analise = data.get('resultados')
    perfil_id = data.get('perfil_id')
    perfil_nome = data.get('perfil_nome')

    if not all([resultados_analise, perfil_id, perfil_nome]):
        return jsonify({"erro": "Dados insuficientes"}), 400

    try:
        df_analise = pd.DataFrame(resultados_analise)
        
        get_final_id = lambda row: (str(row.get('ID Encontrado', '')).strip() or str(row.get('ID Sugerido', '')).strip()) or '❗ Não encontrado'
        def get_final_funcionalidade(row):
            sugestao_var = str(row.get('Sugestão Similar (VAR)', '')).strip()
            return sugestao_var if row.get('Status') in ['Encontrado', 'Divergente com Sugestão'] and sugestao_var else row.get('Funcionalidade Analisada', '')

        df_importacao = pd.DataFrame()
        df_importacao['funcionalidade'] = df_analise.apply(get_final_funcionalidade, axis=1)
        df_importacao['funcionalidade id'] = df_analise.apply(get_final_id, axis=1)
        df_importacao['id'] = perfil_id
        df_importacao['perfil'] = perfil_nome
        df_importacao = df_importacao[['id', 'perfil', 'funcionalidade id', 'funcionalidade']]
        
        output = io.BytesIO()
        with pd.ExcelWriter(output, engine='xlsxwriter') as writer:
            df_importacao.to_excel(writer, index=False, sheet_name='Importa VAR')
        output.seek(0)
        
        data_formatada = datetime.now().strftime("%Y-%m-%d_%H-%M")
        nome_arquivo = f"Importar_VAR_{secure_filename(perfil_nome)}_{data_formatada}.xlsx"

        return send_file(output, as_attachment=True, download_name=nome_arquivo, mimetype='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet')
    except Exception as e:
        app.logger.error("Erro ao gerar planilha de importação", exc_info=True)
        return jsonify({"erro": "Ocorreu um erro interno ao gerar o arquivo."}), 500

# --- ROTAS DO ANALISADOR DE RISCOS SoD (Atualizada) ---
@app.route('/sod_analyzer', methods=['GET', 'POST'])
def sod_analyzer():
    if request.method == 'POST':
        if 'file' not in request.files:
            flash('Nenhum arquivo enviado.', 'danger')
            return redirect(url_for('sod_analyzer'))
        file = request.files['file']
        if file.filename == '':
            flash('Nenhum arquivo selecionado.', 'danger')
            return redirect(url_for('sod_analyzer'))
        if file and allowed_file(file.filename):
            filename = secure_filename(file.filename)
            filepath = os.path.join(app.config['UPLOAD_FOLDER'], filename)
            file.save(filepath)
            
            cenario = request.form.get('analysis_type', 'manutencao')
            resultado_analise = analisar_riscos_excel(filepath, cenario)
            
            os.remove(filepath)
            resultado_analise['nome_arquivo'] = filename
            
            return render_template('sod_analyzer/index.html', resultado=resultado_analise, has_result=True, active_app='sod_analyzer')
        else:
            flash('Tipo de arquivo não permitido. Por favor, envie um arquivo .xlsx ou .xls.', 'danger')
            return redirect(url_for('sod_analyzer'))
    
    return render_template('sod_analyzer/index.html', has_result=False, active_app='sod_analyzer')


if __name__ == '__main__':
    app.run(debug=True)

