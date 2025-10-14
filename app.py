import os
import sqlite3
import pandas as pd
import io
from datetime import datetime
from flask import Flask, render_template, jsonify, request, redirect, url_for, send_from_directory, send_file, flash
from werkzeug.utils import secure_filename
from rapidfuzz import process, fuzz
import traceback

# --- Configuração Inteligente de Caminhos (para funcionar no Render) ---
# Procura pela variável de ambiente do Render; se não achar, usa o diretório local.
DATA_DIR = os.environ.get('RENDER_DISK_PATH', os.path.abspath(os.path.dirname(__file__)))
UPLOAD_FOLDER = os.path.join(DATA_DIR, 'uploads')
OUTPUT_FOLDER = os.path.join(DATA_DIR, 'outputs')
DATABASE_PATH = os.path.join(DATA_DIR, 'sistema.db')

ALLOWED_EXTENSIONS = {'xlsx'}
DEBUG_MODE = True
LIMIAR_ALERTA = 0.90 

MODULOS_PADRAO = [
    'TOTVS Educacional', 'TOTVS Folha de Pagamento', 'TOTVS Gestão Contábil',
    'TOTVS Gestão de Estoque, Compras e Faturamento', 'TOTVS Gestão de Pessoas',
    'TOTVS Gestão Financeira', 'TOTVS Gestão Fiscal', 'TOTVS Gestão Patrimonial',
    'TOTVS Inteligência de Negócios'
]

app = Flask(__name__)
# Para produção, a chave secreta deve ser uma variável de ambiente, mas um valor padrão funciona para começar.
app.secret_key = os.environ.get('SECRET_KEY', os.urandom(24))
app.config.update(
    UPLOAD_FOLDER=UPLOAD_FOLDER,
    OUTPUT_FOLDER=OUTPUT_FOLDER,
    DATABASE=DATABASE_PATH
)

def get_db_connection():
    conn = sqlite3.connect(app.config['DATABASE'])
    conn.row_factory = sqlite3.Row
    return conn

def init_db():
    conn = get_db_connection()
    print("Verificando e criando tabelas do banco de dados se necessário...")
    conn.execute('''
        CREATE TABLE IF NOT EXISTS uploads_historico (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            nome_arquivo_original TEXT NOT NULL,
            nome_arquivo_salvo TEXT NOT NULL,
            timestamp DATETIME NOT NULL,
            status TEXT NOT NULL 
        )
    ''')
    conn.execute('CREATE TABLE IF NOT EXISTS dados_var ("ID Funcionalidade" TEXT, "Funcionalidade" TEXT, "ID Módulo" TEXT, "Módulo" TEXT)')
    conn.commit()
    conn.close()

def setup():
    os.makedirs(UPLOAD_FOLDER, exist_ok=True)
    os.makedirs(OUTPUT_FOLDER, exist_ok=True)
    init_db()

setup()

@app.route('/')
def index():
    conn = get_db_connection()
    historico_bruto = conn.execute('SELECT * FROM uploads_historico ORDER BY timestamp DESC').fetchall()
    
    historico_formatado = []
    for item in historico_bruto:
        item_dict = dict(item)
        try:
            dt_object = datetime.fromisoformat(item_dict['timestamp'])
            item_dict['timestamp_formatado'] = dt_object.strftime('%d/%m/%Y às %H:%M')
        except (ValueError, TypeError):
            item_dict['timestamp_formatado'] = item_dict['timestamp']
        historico_formatado.append(item_dict)

    modulos_disponiveis = []
    is_var_active = False
    try:
        if conn.execute("SELECT count(*) FROM sqlite_master WHERE type='table' AND name='dados_var'").fetchone()[0] == 1:
            if conn.execute('SELECT COUNT(1) FROM dados_var').fetchone()[0] > 0:
                is_var_active = True
        
        if is_var_active:
            modulos_df = pd.read_sql_query('SELECT DISTINCT "Módulo" FROM dados_var', conn)
            if not modulos_df.empty:
                modulos_disponiveis = modulos_df["Módulo"].dropna().sort_values().tolist()
    except Exception as e:
        app.logger.warning(f"Não foi possível carregar módulos do DB. Erro: {e}", exc_info=True)
    
    if not modulos_disponiveis:
        modulos_disponiveis = MODULOS_PADRAO
        
    conn.close()
    return render_template('index.html', historico=historico_formatado, modulos=modulos_disponiveis, is_var_active=is_var_active)

def allowed_file(filename):
    return '.' in filename and filename.rsplit('.', 1)[1].lower() in ALLOWED_EXTENSIONS

@app.route('/upload_var', methods=['POST'])
def upload_var():
    if 'file' not in request.files or not request.files['file'].filename:
        flash('Nenhum arquivo selecionado.', 'warning')
        return redirect(url_for('index', tab='gerenciar'))
    
    file = request.files['file']
    if not allowed_file(file.filename):
        flash('Tipo de arquivo inválido. Por favor, envie um arquivo .xlsx.', 'danger')
        return redirect(url_for('index', tab='gerenciar'))

    original_filename = secure_filename(file.filename)
    
    try:
        df = pd.read_excel(file)
        colunas_esperadas = ["id", "funcionalidade", "modulo id", "modulo"]
        if not all(col in df.columns for col in colunas_esperadas):
            colunas_faltantes = [col for col in colunas_esperadas if col not in df.columns]
            mensagem_erro = (f"Arquivo inválido! Colunas não encontradas: {', '.join(colunas_faltantes)}. A estrutura correta é: id, funcionalidade, modulo id, modulo.")
            flash(mensagem_erro, 'danger')
            return redirect(url_for('index', tab='gerenciar'))
    except Exception as e:
        flash(f"Erro ao ler o arquivo Excel. Verifique o formato. Detalhe: {e}", 'danger')
        return redirect(url_for('index', tab='gerenciar'))

    timestamp = datetime.now()
    saved_filename = f"{timestamp.strftime('%Y-%m-%d_%H-%M-%S')}_{original_filename}"
    filepath = os.path.join(app.config['UPLOAD_FOLDER'], saved_filename)
    file.seek(0)
    file.save(filepath)
    conn = get_db_connection()
    conn.execute(
        'INSERT INTO uploads_historico (nome_arquivo_original, nome_arquivo_salvo, timestamp, status) VALUES (?, ?, ?, "Válido")',
        (original_filename, saved_filename, timestamp)
    )
    conn.commit()
    conn.close()
    
    flash("Arquivo enviado e validado com sucesso!", 'success')
    return redirect(url_for('index', tab='gerenciar'))

@app.route('/ativar_var/<int:upload_id>')
def ativar_var(upload_id):
    conn = get_db_connection()
    upload_para_ativar = conn.execute('SELECT * FROM uploads_historico WHERE id = ? AND status IN ("Válido", "Arquivado")', (upload_id,)).fetchone()
    if upload_para_ativar:
        try:
            filepath = os.path.join(app.config['UPLOAD_FOLDER'], upload_para_ativar['nome_arquivo_salvo'])
            df = pd.read_excel(filepath)
            colunas_do_seu_excel = ["id", "funcionalidade", "modulo id", "modulo"]
            mapa_de_nomes = { "id": "ID Funcionalidade", "funcionalidade": "Funcionalidade", "modulo id": "ID Módulo", "modulo": "Módulo" }
            df_renomeado = df[colunas_do_seu_excel].rename(columns=mapa_de_nomes)
            df_renomeado.to_sql('dados_var', conn, if_exists='replace', index=False)
            conn.execute('UPDATE uploads_historico SET status = "Arquivado" WHERE status = "Ativo"')
            conn.execute('UPDATE uploads_historico SET status = "Ativo" WHERE id = ?', (upload_id,))
            conn.commit()
            flash(f"Planilha '{upload_para_ativar['nome_arquivo_original']}' ativada com sucesso!", 'success')
        except Exception as e:
            app.logger.error(f"Falha ao ativar a planilha ID {upload_id}", exc_info=True)
            flash(f"ERRO AO ATIVAR: Falha ao processar o arquivo.", 'danger')
            conn.execute('UPDATE uploads_historico SET status = "Inválido" WHERE id = ?', (upload_id,))
            conn.commit()
    else:
        flash("Arquivo para ativação não encontrado ou inválido.", 'danger')
    conn.close()
    return redirect(url_for('index', tab='gerenciar'))

@app.route('/comparar', methods=['POST'])
def comparar():
    try:
        conn = get_db_connection()
        var_table_exists = conn.execute("SELECT count(*) FROM sqlite_master WHERE type='table' AND name='dados_var'").fetchone()[0]
        if var_table_exists == 1:
            if conn.execute('SELECT COUNT(1) FROM dados_var').fetchone()[0] == 0:
                conn.close()
                return jsonify({'erro': 'Nenhuma Planilha VAR está ativa. Por favor, vá para a aba "Gerenciar" e ative uma base de dados primeiro.'}), 400
        else:
            conn.close()
            return jsonify({'erro': 'O banco de dados principal (Planilha VAR) ainda não foi criado. Ative uma base na aba "Gerenciar".'}), 400
        conn.close()
    except Exception as e:
        app.logger.error("Erro inesperado na verificação inicial do DB na rota /comparar", exc_info=True)
        return jsonify({'erro': 'Ocorreu um erro ao verificar a base de dados.'}), 500

    if 'arquivo_analise' not in request.files: return jsonify({'erro': 'Nenhum arquivo de análise enviado.'}), 400
    
    try:
        arquivo_usuario = request.files['arquivo_analise']
        modulo_selecionado = request.form.get('modulo')
        if not modulo_selecionado or not arquivo_usuario.filename: return jsonify({'erro': 'Módulo ou arquivo inválido.'}), 400
        normalize = lambda s: str(s).lower().strip()
        df_usuario = pd.read_excel(arquivo_usuario, usecols=[0])
        lista_func_usuario = [normalize(func) for func in df_usuario.iloc[:, 0].dropna()]
        if not lista_func_usuario: return jsonify({'mensagem': 'Nenhuma funcionalidade para analisar na planilha enviada.'})
        conn = get_db_connection()
        query = "SELECT \"ID Funcionalidade\", \"Funcionalidade\" FROM dados_var WHERE \"Módulo\" = ?"
        df_var = pd.read_sql_query(query, conn, params=(modulo_selecionado,))
        conn.close()
        df_var['Funcionalidade_norm'] = df_var['Funcionalidade'].apply(normalize)
        func_var_map = {row['Funcionalidade_norm']: {'id': row['ID Funcionalidade'], 'nome_original': row['Funcionalidade']} for _, row in df_var.iterrows()}
        lista_func_var_norm = df_var['Funcionalidade_norm'].tolist()
    except Exception as e:
        app.logger.error("Erro inesperado ao ler arquivos e preparar dados para comparação", exc_info=True)
        return jsonify({'erro': 'Falha interna no servidor ao processar os arquivos.'}), 500

    resultados_finais, encontrados, divergentes = [], 0, 0
    for func_usuario_norm in lista_func_usuario:
        if func_usuario_norm in func_var_map:
            match = func_var_map[func_usuario_norm]
            resultados_finais.append({'Funcionalidade Analisada': func_usuario_norm, 'Status': 'Encontrado', 'ID Encontrado': match['id'], 'Sugestão Similar (VAR)': match['nome_original'], 'Similaridade (%)': 100.0})
            encontrados += 1
        else:
            divergentes += 1
            best_match = process.extractOne(func_usuario_norm, lista_func_var_norm, scorer=fuzz.WRatio, score_cutoff=80)
            if best_match:
                sugestao_norm, score, _ = best_match
                match = func_var_map[sugestao_norm]
                resultados_finais.append({'Funcionalidade Analisada': func_usuario_norm, 'Status': 'Divergente com Sugestão', 'ID Encontrado': '', 'Sugestão Similar (VAR)': match['nome_original'], 'Similaridade (%)': round(score, 2)})
            else:
                resultados_finais.append({'Funcionalidade Analisada': func_usuario_norm, 'Status': 'Divergente', 'ID Encontrado': '', 'Sugestão Similar (VAR)': '', 'Similaridade (%)': 0.0})

    df_relatorio = pd.DataFrame(resultados_finais)
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    nome_relatorio = f"Relatorio_Analise_{secure_filename(modulo_selecionado)}_{timestamp}.xlsx"
    caminho_relatorio = os.path.join(app.config['OUTPUT_FOLDER'], nome_relatorio)
    df_relatorio.to_excel(caminho_relatorio, index=False)
    analysis_id = timestamp 
    caminho_json_resultados = os.path.join(app.config['OUTPUT_FOLDER'], f"resultados_{analysis_id}.json")
    df_relatorio.to_json(caminho_json_resultados, orient='records', force_ascii=False)

    total_itens = len(resultados_finais)
    is_perfect_match = (total_itens > 0 and encontrados == total_itens)
    if is_perfect_match:
        mensagem = f"✅ Análise finalizada com sucesso!\n\n✔️ {encontrados} funcionalidades encontradas"
    else:
        mensagem = f"⚠️ Análise finalizada com ressalvas!\n\n✔️ {encontrados} encontradas\n❌ {divergentes} divergentes/com sugestão"

    json_response = {'show_import_button': is_perfect_match}
    if DEBUG_MODE:
        user_debug_filename = f"debug_usuario_{timestamp}.txt"
        with open(os.path.join(OUTPUT_FOLDER, user_debug_filename), 'w', encoding='utf-8') as f:
            for item in lista_func_usuario: f.write(f"{item}\n")
        var_debug_filename = f"debug_var_{timestamp}.txt"
        with open(os.path.join(OUTPUT_FOLDER, var_debug_filename), 'w', encoding='utf-8') as f:
            for item in lista_func_var_norm: f.write(f"{item}\n")
        json_response['debug_urls'] = {'usuario': url_for('download_file', filename=user_debug_filename), 'var': url_for('download_file', filename=var_debug_filename)}
    
    if total_itens > 0:
        nao_encontrados = total_itens - encontrados
        ratio_divergencia = nao_encontrados / total_itens
        if total_itens > 10 and ratio_divergencia >= LIMIAR_ALERTA:
            taxa_divergencia = round(ratio_divergencia * 100)
            alerta_msg = (f"Atenção: {taxa_divergencia}% das funcionalidades não foram encontradas. Você tem certeza que selecionou o módulo correto ('{modulo_selecionado}')?")
            json_response['alerta_modulo'] = alerta_msg

    json_response.update({'mensagem': mensagem, 'resultados': resultados_finais, 'download_url': url_for('download_file', filename=nome_relatorio), 'analysis_id': analysis_id})
    return jsonify(json_response)

@app.route('/gerar_importacao', methods=['POST'])
def gerar_importacao():
    data = request.get_json()
    analysis_id, perfil_id, perfil_nome = data.get('analysis_id'), data.get('perfil_id'), data.get('perfil_nome')
    if not all([analysis_id, perfil_id, perfil_nome]): return jsonify({"erro": "Dados insuficientes"}), 400
    try:
        caminho_json_resultados = os.path.join(app.config['OUTPUT_FOLDER'], f"resultados_{analysis_id}.json")
        df_analise = pd.read_json(caminho_json_resultados, orient='records')
        df_importacao = pd.DataFrame()
        df_importacao['funcionalidade'] = df_analise['Funcionalidade Analisada']
        df_importacao['funcionalidade id'] = df_analise.apply(lambda row: row['ID Encontrado'] if row['Status'] == 'Encontrado' else '❗ Não encontrado', axis=1)
        df_importacao['id'] = perfil_id
        df_importacao['perfil'] = perfil_nome
        df_importacao = df_importacao[['id', 'perfil', 'funcionalidade id', 'funcionalidade']]
        output = io.BytesIO()
        with pd.ExcelWriter(output, engine='xlsxwriter') as writer:
            df_importacao.to_excel(writer, index=False, sheet_name='Importa VAR')
        output.seek(0)
        data_formatada = datetime.now().strftime("%Y-%m-%d_%H-%M")
        nome_arquivo = f"Importar_VAR_{secure_filename(perfil_nome)}_{data_formatada}.xlsx"
        return send_file(output, mimetype='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet', as_attachment=True, download_name=nome_arquivo)
    except FileNotFoundError:
        return jsonify({"erro": "Análise não encontrada."}), 404
    except Exception as e:
        app.logger.error("Erro ao gerar planilha de importação", exc_info=True)
        return jsonify({"erro": "Ocorreu um erro interno"}), 500

@app.route('/download/<filename>')
def download_file(filename):
    return send_from_directory(app.config["OUTPUT_FOLDER"], filename, as_attachment=True)

if __name__ == '__main__':
    app.run(debug=True)