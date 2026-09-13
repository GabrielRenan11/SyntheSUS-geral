import gc
import io
import os
import re
import unicodedata
import zipfile
from datetime import datetime

import pandas as pd
import oci
import requests
import urllib3



# ============================================================
# CONFIGURAÇÕES
# ============================================================

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

PATH_CONFIG = os.getenv("OCI_CONFIG_FILE", "config")

BUCKET_BRONZE = os.getenv(
    "BUCKET_BRONZE",
    "synthesus-bronze",
)

BUCKET_SILVER = os.getenv(
    "BUCKET_SILVER",
    "synthesus-silver",
)

# Catálogo CID-10.
# No Airflow/Docker, a forma mais estável é montar/copiar o ZIP para o
# container e informar o caminho por CID10_LOCAL.
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
CID10_LOCAL = os.getenv(
    "CID10_LOCAL",
    os.path.join(BASE_DIR, "CID10CSV.zip"),
)
CID10_ARQUIVO = os.getenv(
    "CID10_ARQUIVO",
    "CID-10-SUBCATEGORIAS.CSV",
)
CID10_OBRIGATORIO = (
    os.getenv("CID10_OBRIGATORIO", "false")
    .strip()
    .lower()
    in {"1", "true", "sim", "yes"}
)

# Base oficial CBO2002 - Ocupação (MTE).
CBO_URL = os.getenv(
    "CBO_URL",
    (
        "https://www.gov.br/trabalho-e-emprego/pt-br/assuntos/"
        "cbo/servicos/downloads/cbo2002-ocupacao.csv"
    ),
)
CBO_LOCAL = os.getenv(
    "CBO_LOCAL",
    "/tmp/CBO2002_OCUPACAO.csv",
)
CBO_OBRIGATORIO = (
    os.getenv("CBO_OBRIGATORIO", "false")
    .strip()
    .lower()
    in {"1", "true", "sim", "yes"}
)

# Janela móvel: últimos N meses COMPLETOS, terminando no mês passado.
QTD_MESES = int(os.getenv("QTD_MESES", "12"))

# Limites aceitos para qualquer campo de data.
DATA_MIN = pd.Timestamp(
    os.getenv("DATA_MIN", "1910-01-01")
)
DATA_MAX = pd.Timestamp(
    os.getenv("DATA_MAX", "2028-12-31")
)

# IDs artificiais:
# cada competência recebe uma faixa própria de 10.000.000 IDs.
ANO_BASE_ID = int(os.getenv("ANO_BASE_ID", "2000"))
TAMANHO_BLOCO_ID = int(
    os.getenv("TAMANHO_BLOCO_ID", "10000000")
)


# ============================================================
# DICIONÁRIOS DE TRADUÇÃO
# ============================================================
#
# O DATASUS trabalha com códigos textuais e zeros à esquerda.
# Portanto, os mapas abaixo usam "01", "02", "00" etc.
# Valores ainda não cadastrados no mapa são preservados no código original.
# ============================================================

MAP_SEXO = {
    # Códigos observados no SIH e variantes já tratadas pelo pipeline anterior.
    "1": "Masculino",
    "01": "Masculino",
    "2": "Feminino",
    "02": "Feminino",
    "3": "Feminino",
    "03": "Feminino",
    "00": "Não informado",
    "09": "Não informado",
}

MAP_RACA_COR = {
    "00": "Não informado",
    "01": "Branca",
    "02": "Preta",
    "03": "Parda",
    "04": "Amarela",
    "05": "Indígena",
    "99": "Não informado",
}

MAP_MARCA_UTI = {
    "00": "Não utilizou UTI",
    "01": "Utilizou mais de um tipo de UTI",
    "51": "UTI adulto - tipo II COVID-19",
    "52": "UTI pediátrica - tipo II COVID-19",
    "74": "UTI adulto - tipo I",
    "75": "UTI adulto - tipo II",
    "76": "UTI adulto - tipo III",
    "77": "UTI infantil - tipo I",
    "78": "UTI infantil - tipo II",
    "79": "UTI infantil - tipo III",
    "80": "UTI neonatal - tipo I",
    "81": "UTI neonatal - tipo II",
    "82": "UTI neonatal - tipo III",
    "83": "UTI de queimados",
    "85": "UTI coronariana tipo II - UCO tipo II",
    "86": "UTI coronariana tipo III - UCO tipo III",
    "99": "UTI Doador",
}

MAP_CAR_INT = {
    "01": "Eletivo",
    "02": "Urgência",
    "03": "Acidente no local de trabalho ou a serviço da empresa",
    "04": "Acidente no trajeto para o trabalho",
    "05": "Outro tipo de acidente de trânsito",
    "06": "Outras lesões/envenenamentos por agentes químicos ou físicos",
}

MAP_CLIENTEL = {
    "00": "Fluxo de clientela não exigido",
    "01": "Atendimento de demanda espontânea",
    "02": "Atendimento de demanda referenciada",
    "03": "Atendimento de demanda espontânea e referenciada",
}

MAP_TP_UNID = {
    "00": "Outros",
    "01": "Posto de Saúde",
    "02": "Centro de Saúde / Unidade Básica",
    "04": "Policlínica",
    "05": "Hospital Geral",
    "07": "Hospital Especializado",
    "09": "Pronto Socorro de Hospital Geral (antigo)",
    "12": "Pronto Socorro Traumato-Ortopédico (antigo)",
    "15": "Unidade Mista",
    "20": "Pronto Socorro Geral",
    "21": "Pronto Socorro Especializado",
    "22": "Consultório Isolado",
    "32": "Unidade Móvel Fluvial",
    "36": "Clínica / Centro de Especialidade",
    "39": "Unidade de Apoio Diagnose e Terapia (SADT Isolado)",
    "40": "Unidade Móvel Terrestre",
    "42": "Unidade Móvel de Nível Pré-Hospitalar na Área de Urgência",
    "43": "Farmácia",
    "45": "Unidade de Saúde da Família",
    "50": "Unidade de Vigilância em Saúde",
    "60": "Cooperativa ou Empresa de Cessão de Trabalhadores na Saúde",
    "61": "Centro de Parto Normal - Isolado",
    "62": "Hospital / Dia - Isolado",
    "63": "Unidade Autorizadora",
    "64": "Central de Regulação de Serviços de Saúde",
    "65": "Unidade de Vigilância Epidemiológica",
    "66": "Unidade de Vigilância Sanitária",
    "67": "Laboratório Central de Saúde Pública - LACEN",
    "68": "Central de Gestão em Saúde",
    "69": "Centro de Atenção Hemoterapia e/ou Hematológica",
    "70": "Centro de Atenção Psicossocial",
    "71": "Centro de Apoio à Saúde da Família",
    "72": "Unidade de Atenção à Saúde Indígena",
    "73": "Pronto Atendimento",
    "74": "Polo Academia da Saúde",
    "75": "Telessaúde",
    "76": "Central de Regulação Médica das Urgências",
    "77": "Serviço de Atenção Domiciliar Isolado (Home Care)",
    "78": "Unidade de Atenção em Regime Residencial",
    "79": "Oficina Ortopédica",
    "80": "Laboratório de Saúde Pública",
    "81": "Central de Regulação do Acesso",
    "82": "Central de Notificação, Captação e Distribuição de Órgãos Estadual",
    "83": "Polo de Prevenção de Doenças e Agravos e Promoção da Saúde",
    "84": "Central de Abastecimento",
    "85": "Centro de Imunização",
}

MAP_SIM_NAO = {
    "00": "Não",
    "01": "Sim",
    "S": "Sim",
    "N": "Não",
    "SIM": "Sim",
    "NAO": "Não",
    "NÃO": "Não",
}

MAP_CODLEITO = {
    "01": "Buco maxilo facial",
    "02": "Cardiologia",
    "03": "Cirurgia geral",
    "04": "Endocrinologia",
    "05": "Gastroenterologia",
    "06": "Ginecologia",
    "07": "Cirúrgico / diagnóstico / terapêutico",
    "08": "Nefrologia / urologia",
    "09": "Neurocirurgia",
    "10": "Obstetrícia cirúrgica",
    "11": "Oftalmologia",
    "12": "Oncologia",
    "13": "Ortopedia / traumatologia",
    "14": "Otorrinolaringologia",
    "15": "Cirurgia plástica",
    "16": "Cirurgia torácica",
    "31": "AIDS",
    "32": "Cardiologia",
    "33": "Clínica geral",
    "34": "Crônicos",
    "35": "Dermatologia",
    "36": "Geriatria",
    "37": "Hansenologia",
    "38": "Hematologia",
    "39": "Leito-dia",
    "40": "Nefrourologia",
    "41": "Neonatologia",
    "42": "Neurologia",
    "43": "Obstetrícia clínica",
    "44": "Oncologia",
    "45": "Pediatria clínica",
    "46": "Pneumologia",
    "47": "Psiquiatria",
    "48": "Reabilitação",
    "49": "Pneumologia sanitária",
    "51": "UTI II adulto - SRAG / COVID-19",
    "52": "UTI II pediátrica - SRAG / COVID-19",
    "61": "UTI adulto",
    "62": "UTI infantil",
    "63": "UTI neonatal",
    "64": "Unidade intermediária",
    "65": "Unidade intermediária neonatal",
    "66": "Unidade de isolamento",
    "67": "Transplante",
    "68": "Pediatria cirúrgica",
    "69": "Hospital-dia - AIDS",
    "70": "Hospital-dia - fibrose cística",
    "71": "Hospital-dia - intercorrência pós-transplante",
    "72": "Hospital-dia - geriatria",
    "73": "Hospital-dia - saúde mental",
    "74": "UTI adulto - tipo I",
    "75": "UTI adulto - tipo II",
    "76": "UTI adulto - tipo III",
    "77": "UTI pediátrica - tipo I",
    "78": "UTI pediátrica - tipo II",
    "79": "UTI pediátrica - tipo III",
    "80": "UTI neonatal - tipo I",
    "81": "UTI neonatal - tipo II",
    "82": "UTI neonatal - tipo III",
    "83": "UTI de queimados",
    "84": "Acolhimento noturno",
    "85": "UTI coronariana tipo II - UCO tipo II",
    "86": "UTI coronariana tipo III - UCO tipo III",
    "87": "Saúde mental",
    "88": "Queimado adulto",
    "89": "Queimado pediátrico",
    "90": "Queimado adulto",
    "91": "Queimado pediátrico",
    "92": "Cuidados intermediários neonatal convencional",
    "93": "Cuidados intermediários neonatal canguru",
    "94": "Cuidados intermediários pediátrico",
    "95": "Cuidados intermediários adulto",
    "96": "Suporte ventilatório pulmonar - COVID-19",
}


# ============================================================
# EQUIPAMENTOS CNES - TIPEQUIP + CODEQUIP
# ============================================================
#
# IMPORTANTE:
# CODEQUIP NÃO é globalmente único.
# O significado correto depende da combinação:
#     (TIPEQUIP, CODEQUIP)
#
# Por isso, os códigos brutos são preservados e as descrições
# são adicionadas em colunas separadas.
# ============================================================

MAP_TIPEQUIP = {
    "1": "Diagnóstico por imagem",
    "2": "Equipamentos de infraestrutura",
    "3": "Equipamentos por métodos ópticos",
    "4": "Equipamentos por métodos gráficos",
    "5": "Manutenção da vida",
    "6": "Outros equipamentos",
    "7": "Odontologia",
    "8": "Audiologia",
    "9": "Telessaúde por imagem",
    "10": "Diálise",
    "11": "Avaliação antropométrica e funcional",
    "12": "Radioterapia",
    "13": "Quimioterapia",
    "14": "Reabilitação",
    "15": "Procedimentos clínicos",
    "16": "Procedimentos cirúrgicos",
}


MAP_CODEQUIP = {
    # 1 - DIAGNÓSTICO POR IMAGEM
    ("1", "01"): "Gama Câmara",
    ("1", "02"): "Mamógrafo com Comando Simples",
    ("1", "03"): "Mamógrafo com Estereotaxia",
    ("1", "04"): "Raio X até 100 mA",
    ("1", "05"): "Raio X de 100 a 500 mA",
    ("1", "06"): "Raio X mais de 500 mA",
    ("1", "07"): "Raio X Odontológico",
    ("1", "08"): "Raio X com Fluoroscopia",
    ("1", "09"): "Raio X para Densitometria Óssea",
    ("1", "10"): "Raio X para Hemodinâmica",
    ("1", "11"): "Tomógrafo Computadorizado",
    ("1", "12"): "Ressonância Magnética",
    ("1", "13"): "Ultrassom Doppler Colorido",
    ("1", "14"): "Ultrassom Ecógrafo",
    ("1", "15"): "Ultrassom Convencional",
    ("1", "16"): "Processadora de Filme Exclusiva para Mamografia",
    ("1", "17"): "Mamógrafo Digital",
    ("1", "18"): "PET/CT",
    ("1", "19"): "Mamógrafo com Tomossíntese",
    ("1", "20"): "Raio X Analógico",
    ("1", "21"): "Raio X Digital",
    ("1", "22"): "Raio X Telecomandado",
    ("1", "23"): "Raio X Móvel",
    ("1", "24"): "Arco Cirúrgico",
    ("1", "25"): "Raio X Panorâmico",
    ("1", "26"): "Tomógrafo Computadorizado 4 Canais",
    ("1", "27"): "Tomógrafo Computadorizado 16 Canais",
    ("1", "28"): "Tomógrafo Computadorizado 32 Canais",
    ("1", "29"): "Tomógrafo Computadorizado 64 Canais",
    ("1", "30"): "Tomógrafo Computadorizado 128 Canais",
    ("1", "31"): "Tomógrafo Simulador para Radioterapia (Uso Exclusivo)",
    ("1", "32"): "Ressonância Magnética 0.5T",
    ("1", "33"): "Ressonância Magnética 1.5T",
    ("1", "34"): "Ressonância Magnética 3T",
    ("1", "35"): "Ressonância Magnética de Campo Aberto",

    # 2 - EQUIPAMENTOS DE INFRAESTRUTURA
    ("2", "19"): "Ar Condicionado",
    ("2", "20"): "Câmara Frigorífica",
    ("2", "21"): "Controle Ambiental / Ar-condicionado Central",
    ("2", "22"): "Grupo Gerador",
    ("2", "23"): "Usina de Oxigênio",
    ("2", "24"): "Câmara para Conservação de Hemoderivados/Imuno/Termolábeis",
    ("2", "25"): "Câmara para Conservação de Imunobiológicos",
    ("2", "26"): "Condensador",
    ("2", "27"): "Freezer Científico",
    ("2", "28"): "Grupo Gerador (101 a 300 KVA)",
    ("2", "29"): "Grupo Gerador (8 a 100 KVA)",
    ("2", "30"): "Grupo Gerador (Acima de 300 KVA)",
    ("2", "43"): "Grupo Gerador de 1.500 KVA (Mínimo)",
    ("2", "65"): "Grupo Gerador Portátil (Até 7 KVA)",
    ("2", "66"): "Refrigerador",

    # 3 - EQUIPAMENTOS POR MÉTODOS ÓPTICOS
    ("3", "31"): "Endoscópio das Vias Respiratórias",
    ("3", "32"): "Endoscópio das Vias Urinárias",
    ("3", "33"): "Endoscópio Digestivo",
    ("3", "34"): "Equipamentos para Optometria",
    ("3", "35"): "Laparoscópio/Vídeo",
    ("3", "36"): "Microscópio Cirúrgico",
    ("3", "37"): "Cadeira Oftalmológica",
    ("3", "38"): "Coluna Oftalmológica",
    ("3", "39"): "Refrator",
    ("3", "40"): "Lensômetro",
    ("3", "44"): "Projetor ou Tabela de Optotipos",
    ("3", "45"): "Retinoscópio",
    ("3", "46"): "Oftalmoscópio",
    ("3", "47"): "Ceratômetro",
    ("3", "48"): "Tonômetro de Aplanação",
    ("3", "49"): "Biomicroscópio (Lâmpada de Fenda)",
    ("3", "50"): "Campímetro",
    ("3", "51"): "Histeroscópio",

    # 4 - EQUIPAMENTOS POR MÉTODOS GRÁFICOS
    ("4", "41"): "Eletrocardiógrafo",
    ("4", "42"): "Eletroencefalógrafo",

    # 5 - MANUTENÇÃO DA VIDA
    ("5", "51"): "Bomba/Balão Intra-aórtico",
    ("5", "52"): "Bomba de Infusão",
    ("5", "53"): "Berço Aquecido",
    ("5", "54"): "Bilirrubinômetro",
    ("5", "55"): "Debitômetro",
    ("5", "56"): "Desfibrilador Convencional",
    ("5", "57"): "Equipamento de Fototerapia",
    ("5", "58"): "Incubadora",
    ("5", "59"): "Marca-passo Temporário",
    ("5", "60"): "Monitor de ECG",
    ("5", "61"): "Monitor de Pressão Invasivo",
    ("5", "62"): "Monitor de Pressão Não Invasivo",
    ("5", "63"): "Reanimador Pulmonar/AMBU",
    ("5", "64"): "Respirador/Ventilador",
    ("5", "65"): "Monitor Multiparâmetro",
    ("5", "66"): "Desfibrilador Externo Automático",

    # 6 - OUTROS EQUIPAMENTOS
    ("6", "67"): "Caminhão Baú Refrigerado",
    ("6", "68"): "Embarcação para Transporte com Motor Popa (Até 12 Pessoas)",
    ("6", "69"): "Empilhadeira",
    ("6", "70"): "Veículo Utilitário (Tipo Furgão)",
    ("6", "71"): "Aparelho de Diatermia por Ultrassom/Ondas Curtas",
    ("6", "72"): "Aparelho de Eletroestimulação",
    ("6", "73"): "Bomba de Infusão de Hemoderivados",
    ("6", "74"): "Equipamentos de Aférese",
    ("6", "76"): "Equipamento de Circulação Extracorpórea",
    ("6", "78"): "Forno de Bier",
    ("6", "79"): "Veículo Pick-up Cabine Dupla 4x4 (Diesel)",

    # 7 - ODONTOLOGIA
    ("7", "61"): "Aquecedor Endodôntico para Gutta Percha",
    ("7", "62"): "Articulador Odontológico",
    ("7", "63"): "Bomba a Vácuo Odontológica",
    ("7", "64"): "Câmara Escura Odontológica",
    ("7", "65"): "Consultório Odontológico Portátil",
    ("7", "66"): "Equipo Cart Odontológico",
    ("7", "67"): "Gerador de Ozônio Odontológico",
    ("7", "68"): "Laser para Tratamento Odontológico",
    ("7", "69"): "Localizador de Ápice",
    ("7", "70"): "Micromotor Elétrico com Localizador de Ápice",
    ("7", "71"): "Motor Elétrico Cirúrgico Odontológico",
    ("7", "72"): "Plastificadora para Uso Odontológico",
    ("7", "73"): "Refletor Odontológico",
    ("7", "74"): "Scanner de Bancada CAD/CAM",
    ("7", "75"): "Scanner Intraoral Odontológico",
    ("7", "76"): "Torno Odontológico",
    ("7", "77"): "Ultrassom Odontológico",
    ("7", "78"): "Unidade Auxiliar com Sugador",
    ("7", "79"): "Vibrador de Gesso",
    ("7", "80"): "Cadeira Odontológica Completa",
    ("7", "81"): "Compressor Odontológico",
    ("7", "82"): "Fotopolimerizador de Resinas",
    ("7", "83"): "Caneta de Alta Rotação",
    ("7", "84"): "Caneta de Baixa Rotação",
    ("7", "85"): "Amalgamador Odontológico",
    ("7", "86"): "Aparelho de Profilaxia com Jato de Bicarbonato",
    ("7", "87"): "Centrifugador para Prótese Dentária",
    ("7", "88"): "Forno Fotopolimerizador",
    ("7", "90"): "Fresadora Odontológica",
    ("7", "91"): "Imersor de Cera",
    ("7", "92"): "Impressora 3D Odontológica",
    ("7", "93"): "Martelete Pneumático",
    ("7", "94"): "Microjato para Prótese Dentária",
    ("7", "95"): "Micromotor Elétrico de Bancada",
    ("7", "96"): "Motor de Suspensão",
    ("7", "98"): "Panela Eliminadora de Bolhas",
    ("7", "99"): "Recortador de Gesso",

    # 8 - AUDIOLOGIA
    ("8", "86"): "Otoscópio",
    ("8", "87"): "Emissões Otoacústicas Evocadas Transientes",
    ("8", "88"): "Emissões Otoacústicas Evocadas por Produto de Distorção",
    ("8", "89"): "Potencial Evocado Auditivo de Tronco Encefálico Automático",
    ("8", "90"): "Potencial Evocado Auditivo de Tronco Encefálico de Curta, Média e Longa Latência",
    ("8", "91"): "Audiômetro de Um Canal",
    ("8", "92"): "Audiômetro de Dois Canais",
    ("8", "93"): "Imitanciômetro",
    ("8", "94"): "Imitanciômetro Multifrequencial",
    ("8", "95"): "Cabine Acústica",
    ("8", "96"): "Sistema de Campo Livre",
    ("8", "97"): "Sistema Completo de Reforço Visual (VRA)",
    ("8", "98"): "Ganho de Inserção",
    ("8", "99"): "Hi-Pro",

    # 9 - TELESSAÚDE POR IMAGEM
    ("9", "01"): "Câmera para Reconhecimento Facial",
    ("9", "03"): "Condensador",
    ("9", "04"): "Dermatoscópio",
    ("9", "05"): "Detector Fetal Portátil",
    ("9", "07"): "Kit Médico de Diagnóstico Audiológico",
    ("9", "08"): "Mesa Digitalizadora",
    ("9", "09"): "Monitor de Sinais Vitais Multifuncional Portátil para Telessaúde",
    ("9", "10"): "Retinógrafo Portátil",
    ("9", "11"): "Ultrassom Portátil",
    ("9", "12"): "Eletrocardiógrafo para Telessaúde",
    ("9", "13"): "Espirômetro para Telessaúde",
    ("9", "14"): "Otoscópio para Telessaúde",

    # 10 - DIÁLISE
    ("10", "01"): "Aparelho de Hemodiálise - Ambulatorial",
    ("10", "02"): "Aparelho de Hemodiálise - Hospitalar",
    ("10", "03"): "Aparelho de Hemodiálise Reserva",
    ("10", "04"): "Aparelho para Diálise Peritoneal",

    # 11 - AVALIAÇÃO ANTROPOMÉTRICA E FUNCIONAL
    ("11", "01"): "Adipômetro",
    ("11", "02"): "Analisador de Composição Corporal",
    ("11", "03"): "Balança Antropométrica Adulto até 200 kg",
    ("11", "04"): "Balança Antropométrica Infantil",
    ("11", "05"): "Balança Antropométrica para Obesos acima de 200 kg",
    ("11", "06"): "Balança para Pessoa em Cadeira de Rodas",
    ("11", "07"): "Balança com Bioimpedância",
    ("11", "08"): "Balança Digital Portátil",
    ("11", "09"): "Flexímetro de Banco / Banco de Wells",
    ("11", "10"): "Dinamômetro Digital",
    ("11", "11"): "Estadiômetro",
    ("11", "12"): "Espirômetro Portátil",
    ("11", "13"): "Goniômetro",
    ("11", "14"): "Paquímetro",

    # 12 - RADIOTERAPIA
    ("12", "01"): "Acelerador Linear sem Elétrons (Básico - Intermediário)",
    ("12", "02"): "Acelerador Linear com Elétrons (Recursos Avançados IGRT 3D)",
    ("12", "03"): "Unidade de Cobaltoterapia",
    ("12", "04"): "Braquiterapia",
    ("12", "05"): "Sistema de Planejamento",
    ("12", "06"): "Sistema de Dosimetria",
    ("12", "07"): "Fonte SR90 Selada",

    # 13 - QUIMIOTERAPIA
    ("13", "01"): "Cabine de Segurança Biológica Classe II B2",
    ("13", "02"): "Poltrona para Administração de Quimioterapia",
    ("13", "03"): "Cama Hospitalar para Administração de Quimioterapia",

    # 14 - REABILITAÇÃO
    ("14", "01"): "Barra de Flexão em T",
    ("14", "02"): "Banco Regulável Ajustável Supino - 0 a 90°",
    ("14", "03"): "Banco Supino Reto",
    ("14", "04"): "Barras Paralelas para Fisioterapia",
    ("14", "05"): "Cadeira Adutora e Abdutora",
    ("14", "06"): "Cadeira Flexora e Extensora",
    ("14", "07"): "Cama Elástica Proprioceptiva",
    ("14", "08"): "Disco Flexor",
    ("14", "09"): "Eletroestimulador Funcional TENS/FES",
    ("14", "10"): "Escada Digital em Madeira para Reabilitação",
    ("14", "11"): "Escada em L com Rampa",
    ("14", "12"): "Escada Linear para Marcha sem Rampa",
    ("14", "13"): "Escada Suspensa",
    ("14", "14"): "Espaldar em Madeira / Barra / Escada de Ling",
    ("14", "15"): "Esteira Ergométrica",
    ("14", "16"): "Estimulador Transcutâneo TENS",
    ("14", "17"): "Estimulador Elétrico Funcional FES",
    ("14", "18"): "Gangorra de Equilíbrio",
    ("14", "19"): "Laser Terapêutico de Baixa Potência",
    ("14", "20"): "Simulador de Remo",
    ("14", "21"): "Sistema de Luzes de Treinamento Reativo",
    ("14", "22"): "Tábua de Propriocepção",
    ("14", "23"): "Tábua de Quadríceps",
    ("14", "24"): "Tábua de Tríceps",
    ("14", "25"): "Ultrassom Terapêutico (1 MHz e 3 MHz)",

    # 15 - PROCEDIMENTOS CLÍNICOS
    ("15", "01"): "Doppler Vascular Portátil",
    ("15", "02"): "Eletrocautério até 150 W",
    ("15", "03"): "Fotóforo Clínico",

    # 16 - PROCEDIMENTOS CIRÚRGICOS
    ("16", "01"): "Sistema Cirúrgico Robótico",
}


# Índice auxiliar por tipo para traduzir milhões de linhas sem criar
# uma Series gigante de tuplas (economiza RAM no container).
MAP_CODEQUIP_POR_TIPO = {}

for (tipo_equip, codigo_equip), descricao_equip in MAP_CODEQUIP.items():
    MAP_CODEQUIP_POR_TIPO.setdefault(tipo_equip, {})[codigo_equip] = descricao_equip


# ============================================================
# OCI
# ============================================================

def get_oci_client():
    """
    Cria o cliente OCI usando o config montado no container.

    Variável esperada:
        OCI_CONFIG_FILE=/caminho/para/config
    """
    config = oci.config.from_file(
        PATH_CONFIG,
        "DEFAULT",
    )
    oci.config.validate_config(config)

    client = oci.object_storage.ObjectStorageClient(
        config
    )
    namespace = client.get_namespace().data

    print(
        f"✓ Conectado à OCI com sucesso "
        f"(config: {PATH_CONFIG})!"
    )

    return client, namespace


def listar_nomes_objetos(client, namespace, bucket):
    """Lista TODOS os objetos do bucket, com paginação."""
    print(f"🔎 Listando objetos do bucket '{bucket}'...")

    nomes = []
    start = None

    while True:
        kwargs = {
            "namespace_name": namespace,
            "bucket_name": bucket,
        }

        if start is not None:
            kwargs["start"] = start

        resposta = client.list_objects(**kwargs)
        nomes.extend(obj.name for obj in resposta.data.objects)

        if not resposta.data.next_start_with:
            break

        start = resposta.data.next_start_with

    print(f"✓ {len(nomes):,} objetos encontrados em '{bucket}'.")
    return nomes


def ler_parquet_oci_seguro(client, namespace, bucket, object_name):
    try:
        response = client.get_object(namespace, bucket, object_name)
        return pd.read_parquet(io.BytesIO(response.data.content))

    except oci.exceptions.ServiceError as e:
        if e.status == 404:
            print(f"⚠️ Arquivo '{object_name}' não encontrado em '{bucket}'.")
            return pd.DataFrame()
        raise


def salvar_parquet_oci(client, namespace, bucket, object_name, df):
    if df is None or df.empty:
        print(f"⚠️ '{object_name}' está vazio. Persistência ignorada.")
        return

    print(
        f"📤 Salvando '{object_name}' em '{bucket}' "
        f"({len(df):,} registros)..."
    )

    buffer = io.BytesIO()

    try:
        df.to_parquet(buffer, index=False)
        buffer.seek(0)
        client.put_object(
            namespace_name=namespace,
            bucket_name=bucket,
            object_name=object_name,
            put_object_body=buffer,
        )
    finally:
        buffer.close()


def ler_bronze_competencia(
    client,
    namespace,
    bucket,
    nomes_objetos,
    prefixo,
    competencia,
):
    """
    Consolida somente os arquivos Bronze da competência solicitada.

    Exemplo:
        prefixo='raw_cnes_pf'
        competencia='202509'

    Procura:
        raw_cnes_pf_ac_202509.parquet
        ...
        raw_cnes_pf_to_202509.parquet
    """
    sufixo = f"_{competencia}.parquet"

    selecionados = sorted(
        nome
        for nome in nomes_objetos
        if nome.startswith(prefixo) and nome.endswith(sufixo)
    )

    print(
        f"🔍 {prefixo} | competência {competencia}: "
        f"{len(selecionados)} arquivo(s) Bronze encontrado(s)."
    )

    if not selecionados:
        return pd.DataFrame()

    dfs = []

    for nome in selecionados:
        df_temp = ler_parquet_oci_seguro(
            client,
            namespace,
            bucket,
            nome,
        )

        if not df_temp.empty:
            dfs.append(df_temp)

    if not dfs:
        return pd.DataFrame()

    return pd.concat(dfs, ignore_index=True)


# ============================================================
# COMPETÊNCIAS
# ============================================================

def obter_ultimos_12_meses_completos():
    """
    Retorna os últimos 12 meses completos, terminando no mês passado.

    Em 09/2026:
        202509 ... 202608
    """
    hoje = datetime.now()

    ano = hoje.year
    mes = hoje.month - 1

    if mes == 0:
        mes = 12
        ano -= 1

    competencias = []

    for lag in range(QTD_MESES - 1, -1, -1):
        m = mes - lag
        a = ano

        while m <= 0:
            m += 12
            a -= 1

        competencias.append(f"{a}{m:02d}")

    return competencias


def competencia_para_timestamp(competencia):
    return pd.to_datetime(
        competencia + "01",
        format="%Y%m%d",
        errors="raise",
    )


def converter_competencia_para_data(df, col_comp):
    """Converte YYYYMM ou MMYYYY para o primeiro dia do mês."""
    if col_comp not in df.columns or df.empty:
        return pd.Series(dtype="datetime64[ns]")

    comp_str = (
        df[col_comp]
        .astype("string")
        .str.strip()
        .str.replace(r"\.0$", "", regex=True)
        .str.replace(r"\D", "", regex=True)
    )

    dt_yyyymm = pd.to_datetime(
        comp_str + "01",
        format="%Y%m%d",
        errors="coerce",
    )

    dt_mmyyyy = pd.to_datetime(
        "01" + comp_str,
        format="%d%m%Y",
        errors="coerce",
    )

    return dt_yyyymm.fillna(dt_mmyyyy)


# ============================================================
# LIMPEZA / CÓDIGOS / TRADUÇÃO
# ============================================================

def limpar_data_intervalo(serie, nome_coluna="data"):
    """Transforma datas fora de 1910-2028 em NaT."""
    serie_dt = pd.to_datetime(serie, errors="coerce")

    mascara_outlier = (
        serie_dt.notna()
        & ~serie_dt.between(DATA_MIN, DATA_MAX)
    )

    qtd = int(mascara_outlier.sum())

    if qtd > 0:
        print(
            f"🧹 {qtd:,} data(s) fora de 1910-2028 "
            f"removida(s) da coluna '{nome_coluna}'."
        )

    return serie_dt.mask(mascara_outlier)


def normalizar_codigo(serie):
    """
    Normaliza texto SEM remover zeros à esquerda.

    Exemplos:
        '01' -> '01'
        1.0  -> '1'
    """
    return (
        serie
        .astype("string")
        .str.strip()
        .str.replace(r"\.0$", "", regex=True)
    )


def padronizar_codigo(serie, largura):
    """Mantém/põe zeros à esquerda em códigos exclusivamente numéricos."""
    s = normalizar_codigo(serie)

    mascara_numerica = s.str.fullmatch(r"\d+", na=False)
    s.loc[mascara_numerica] = s.loc[mascara_numerica].str.zfill(largura)

    return s


def normalizar_tipequip(serie):
    """
    Normaliza TIPEQUIP somente para uso nas chaves dos dicionários.

    O arquivo CNES pode trazer:
        01 -> 1
        07 -> 7
        09 -> 9
        10 -> 10
        16 -> 16

    O código original da coluna TIPEQUIP pode continuar preservado na saída.
    """
    s = normalizar_codigo(serie)
    mascara_numerica = s.str.fullmatch(r"\d+", na=False)

    s.loc[mascara_numerica] = (
        s.loc[mascara_numerica]
        .str.lstrip("0")
        .replace("", "0")
    )

    return s


def normalizar_cbo_codigo(serie):
    """
    Normaliza CBO para os 6 dígitos usados na CBO2002.

    Exemplos:
        322205  -> 322205
        3222-05 -> 322205
        3222 05 -> 322205
    """
    s = (
        serie.astype("string")
        .str.strip()
        .str.replace(r"\.0$", "", regex=True)
        .str.replace(r"\D+", "", regex=True)
    )

    s = s.where(s.str.len() > 0, pd.NA)

    mascara_numerica = s.str.fullmatch(r"\d+", na=False)
    s.loc[mascara_numerica] = s.loc[mascara_numerica].str.zfill(6)

    return s


def traduzir_codigo(serie, mapa, nome_coluna, largura=None):
    """
    Traduz códigos conhecidos e preserva o código original nos desconhecidos.
    """
    if largura is None:
        s = normalizar_codigo(serie)
    else:
        s = padronizar_codigo(serie, largura)

    traduzida = s.map(mapa)
    resultado = traduzida.fillna(s)

    desconhecidos = s.notna() & traduzida.isna()

    if desconhecidos.any():
        exemplos = (
            s[desconhecidos]
            .dropna()
            .drop_duplicates()
            .sort_values()
            .head(10)
            .tolist()
        )

        print(
            f"ℹ️ '{nome_coluna}': {int(desconhecidos.sum()):,} "
            f"valor(es) sem tradução cadastrada. "
            f"Exemplos: {exemplos}"
        )

    return resultado


def filtrar_competencia_exata(df, coluna_data, competencia):
    if df.empty or coluna_data not in df.columns:
        return df

    alvo = competencia_para_timestamp(competencia)

    return df[
        (df[coluna_data].dt.year == alvo.year)
        & (df[coluna_data].dt.month == alvo.month)
    ].copy()



# ============================================================
# CATÁLOGO CID-10 - DATASUS
# ============================================================

def carregar_mapa_cid10():
    """
    Carrega o catálogo oficial CID-10 do DATASUS a partir do ZIP local e monta:
        SUBCAT -> DESCRICAO

    No Airflow/Docker, informe o caminho pelo ambiente CID10_LOCAL.

    O arquivo SUBCAT usa o mesmo formato de código do DIAG_PRINC do SIH,
    por exemplo: J189, A000, I10.
    """
    print("📚 Carregando catálogo CID-10 local...")

    if not os.path.isfile(CID10_LOCAL):
        mensagem = (
            f"Arquivo CID-10 não encontrado em: {CID10_LOCAL}. "
            "DIAG_PRINC será preservado e DIAG_DESCRICAO ficará nula."
        )
        if CID10_OBRIGATORIO:
            raise FileNotFoundError(mensagem)

        print(f"⚠️ {mensagem}")
        return {}

    if not zipfile.is_zipfile(CID10_LOCAL):
        mensagem = (
            f"O arquivo '{CID10_LOCAL}' existe, mas não é um ZIP válido. "
            "DIAG_DESCRICAO ficará nula."
        )
        if CID10_OBRIGATORIO:
            raise RuntimeError(mensagem)

        print(f"⚠️ {mensagem}")
        return {}

    with zipfile.ZipFile(CID10_LOCAL) as z:
        candidatos = [
            nome
            for nome in z.namelist()
            if nome.upper().endswith(CID10_ARQUIVO.upper())
        ]

        if not candidatos:
            raise RuntimeError(
                f"Arquivo '{CID10_ARQUIVO}' não encontrado dentro do ZIP CID-10."
            )

        arquivo_cid = candidatos[0]

        with z.open(arquivo_cid) as csv:
            df_cid = pd.read_csv(
                csv,
                sep=";",
                encoding="ISO-8859-1",
                dtype="string",
            )

    # Remove a coluna vazia que pode surgir por causa do ';' final do CSV.
    df_cid.columns = df_cid.columns.str.strip()
    df_cid = df_cid.loc[
        :, ~df_cid.columns.str.startswith("Unnamed")
    ].copy()

    obrigatorias = {"SUBCAT", "DESCRICAO"}
    faltantes = obrigatorias - set(df_cid.columns)

    if faltantes:
        raise RuntimeError(
            "Catálogo CID-10 sem as colunas obrigatórias: "
            + ", ".join(sorted(faltantes))
        )

    df_cid["SUBCAT"] = (
        df_cid["SUBCAT"]
        .astype("string")
        .str.strip()
        .str.upper()
        .str.replace(".", "", regex=False)
    )

    df_cid["DESCRICAO"] = (
        df_cid["DESCRICAO"]
        .astype("string")
        .str.strip()
    )

    df_cid = df_cid[
        df_cid["SUBCAT"].notna()
        & df_cid["DESCRICAO"].notna()
        & (df_cid["SUBCAT"] != "")
        & (df_cid["DESCRICAO"] != "")
    ].copy()

    duplicados = int(df_cid["SUBCAT"].duplicated().sum())

    if duplicados > 0:
        print(
            f"⚠️ CID-10: {duplicados:,} código(s) duplicado(s) no catálogo. "
            "A primeira descrição será mantida."
        )

    df_cid = df_cid.drop_duplicates(
        subset=["SUBCAT"],
        keep="first",
    )

    mapa = df_cid.set_index("SUBCAT")["DESCRICAO"].to_dict()

    print(
        f"✓ Catálogo CID-10 carregado: {len(mapa):,} subcategorias disponíveis."
    )

    return mapa


def adicionar_descricao_cid10(serie, mapa_cid10):
    """
    Retorna somente a descrição CID-10 correspondente ao código.
    O código original permanece preservado em DIAG_PRINC.
    """
    codigos = (
        normalizar_codigo(serie)
        .str.upper()
        .str.replace(".", "", regex=False)
    )

    descricoes = codigos.map(mapa_cid10).astype("string")

    desconhecidos = codigos.notna() & descricoes.isna()

    if desconhecidos.any():
        exemplos = (
            codigos[desconhecidos]
            .dropna()
            .drop_duplicates()
            .sort_values()
            .head(10)
            .tolist()
        )

        print(
            f"ℹ️ 'DIAG_PRINC': {int(desconhecidos.sum()):,} registro(s) "
            "sem descrição CID-10 encontrada. "
            f"Exemplos: {exemplos}"
        )

    return codigos, descricoes


# ============================================================
# CATÁLOGO CBO2002 - MINISTÉRIO DO TRABALHO E EMPREGO
# ============================================================

def normalizar_nome_coluna(texto):
    """Normaliza cabeçalhos para facilitar a leitura do CSV oficial."""
    texto = str(texto).strip().lower()
    texto = unicodedata.normalize("NFKD", texto)
    texto = "".join(
        caractere
        for caractere in texto
        if not unicodedata.combining(caractere)
    )
    texto = re.sub(r"[^a-z0-9]+", "_", texto)
    return texto.strip("_")


def carregar_mapa_cbo():
    """
    Carrega a base oficial CBO2002 - Ocupação do MTE e monta:
        código de 6 dígitos -> título da ocupação

    A base é baixada apenas se CBO_LOCAL ainda não existir no runtime.
    """
    if os.path.isfile(CBO_LOCAL) and os.path.getsize(CBO_LOCAL) > 0:
        print(f"📚 Carregando CBO oficial local: {CBO_LOCAL}")
    else:
        print("🌐 Baixando base oficial CBO2002 - Ocupação do MTE...")

        try:
            resposta = requests.get(
                CBO_URL,
                timeout=(30, 120),
            )
            resposta.raise_for_status()

            if not resposta.content:
                raise RuntimeError(
                    "O MTE retornou um arquivo CBO vazio."
                )

            diretorio_cbo = os.path.dirname(
                os.path.abspath(CBO_LOCAL)
            )
            os.makedirs(
                diretorio_cbo,
                exist_ok=True,
            )

            with open(CBO_LOCAL, "wb") as arquivo:
                arquivo.write(resposta.content)

            print(
                f"✓ CBO oficial salva em {CBO_LOCAL} "
                f"({os.path.getsize(CBO_LOCAL):,} bytes)."
            )

        except Exception as e:
            mensagem = (
                "Não foi possível obter a base oficial CBO2002. "
                f"Os códigos CBO serão preservados sem título. Erro: {e}"
            )

            if CBO_OBRIGATORIO:
                raise RuntimeError(mensagem) from e

            print(f"⚠️ {mensagem}")
            return {}

    # O arquivo oficial é pequeno. O parser tenta as codificações usuais
    # e detecta o separador automaticamente para tolerar mudanças de publicação.
    ultimo_erro = None
    df_cbo = None

    for encoding in ("utf-8-sig", "latin1"):
        try:
            df_cbo = pd.read_csv(
                CBO_LOCAL,
                sep=None,
                engine="python",
                encoding=encoding,
                dtype="string",
            )
            break
        except Exception as e:
            ultimo_erro = e

    if df_cbo is None:
        mensagem = (
            f"Não foi possível ler o CSV oficial da CBO: {ultimo_erro}. "
            "Os códigos CBO serão preservados sem título."
        )
        if CBO_OBRIGATORIO:
            raise RuntimeError(mensagem)

        print(f"⚠️ {mensagem}")
        return {}

    df_cbo.columns = [
        normalizar_nome_coluna(coluna)
        for coluna in df_cbo.columns
    ]

    # A publicação oficial costuma usar CODIGO/TITULO. Mantemos fallback
    # para pequenas alterações de nomenclatura do cabeçalho.
    candidatos_codigo = [
        "codigo",
        "codigo_cbo",
        "cbo",
        "cod_ocupacao",
        "codigo_ocupacao",
    ]
    candidatos_titulo = [
        "titulo",
        "titulo_ocupacao",
        "descricao",
        "descricao_ocupacao",
        "nome",
    ]

    col_codigo = next(
        (c for c in candidatos_codigo if c in df_cbo.columns),
        None,
    )
    col_titulo = next(
        (c for c in candidatos_titulo if c in df_cbo.columns),
        None,
    )

    # Fallback semântico caso o MTE altere levemente o nome dos cabeçalhos.
    if col_codigo is None:
        col_codigo = next(
            (
                c for c in df_cbo.columns
                if "cod" in c and ("ocup" in c or "cbo" in c)
            ),
            None,
        )

    if col_titulo is None:
        col_titulo = next(
            (
                c for c in df_cbo.columns
                if "titul" in c or "descr" in c
            ),
            None,
        )

    if col_codigo is None or col_titulo is None:
        raise RuntimeError(
            "Não foi possível identificar as colunas de código e título "
            f"no CSV oficial da CBO. Colunas encontradas: {df_cbo.columns.tolist()}"
        )

    df_cbo[col_codigo] = normalizar_cbo_codigo(df_cbo[col_codigo])
    df_cbo[col_titulo] = (
        df_cbo[col_titulo]
        .astype("string")
        .str.strip()
    )

    df_cbo = df_cbo[
        df_cbo[col_codigo].notna()
        & df_cbo[col_titulo].notna()
        & (df_cbo[col_titulo] != "")
    ].copy()

    duplicados = int(df_cbo[col_codigo].duplicated().sum())

    if duplicados > 0:
        print(
            f"⚠️ CBO: {duplicados:,} código(s) duplicado(s) na base oficial. "
            "O primeiro título será mantido."
        )

    df_cbo = df_cbo.drop_duplicates(
        subset=[col_codigo],
        keep="first",
    )

    mapa = (
        df_cbo
        .set_index(col_codigo)[col_titulo]
        .to_dict()
    )

    print(f"✓ CBO oficial carregada: {len(mapa):,} ocupações.")

    return mapa


def traduzir_cbo_no_proprio_campo(serie, mapa_cbo):
    """
    Substitui o conteúdo de CBO pelo formato:
        XXXXXX - Título da ocupação

    Não cria coluna nem tabela nova.
    Se o código não existir na base oficial, preserva apenas o código.
    """
    codigos = normalizar_cbo_codigo(serie)
    titulos = codigos.map(mapa_cbo).astype("string")

    resultado = codigos.copy()

    encontrados = codigos.notna() & titulos.notna()

    resultado.loc[encontrados] = (
        codigos.loc[encontrados]
        + " - "
        + titulos.loc[encontrados]
    )

    nao_encontrados = codigos.notna() & titulos.isna()

    if nao_encontrados.any():
        exemplos = (
            codigos.loc[nao_encontrados]
            .drop_duplicates()
            .sort_values()
            .head(10)
            .tolist()
        )

        print(
            f"ℹ️ CBO: {int(nao_encontrados.sum()):,} registro(s) "
            "sem título encontrado na base oficial do MTE. "
            f"Exemplos: {exemplos}"
        )

    return resultado.astype("string")


# ============================================================
# IDs ARTIFICIAIS GLOBAIS POR COMPETÊNCIA
# ============================================================

def calcular_inicio_bloco_id(competencia):
    """
    Gera uma faixa exclusiva de IDs para cada competência.

    Fórmula:
        indice_mes = (ano - 2000) * 12 + mes
        inicio = indice_mes * 10.000.000

    Para o intervalo até 2028, continua cabendo em NUMBER(10).
    """
    ano = int(competencia[:4])
    mes = int(competencia[4:6])

    if ano < ANO_BASE_ID:
        raise ValueError(
            f"Competência {competencia} anterior ao ANO_BASE_ID={ANO_BASE_ID}."
        )

    indice_mes = ((ano - ANO_BASE_ID) * 12) + mes
    return indice_mes * TAMANHO_BLOCO_ID


def adicionar_id_artificial(
    df,
    competencia,
    nome_id,
    colunas_ordenacao,
):
    """
    Cria um ID único POR LINHA e globalmente único entre competências.

    Importante:
    - CNS pode repetir.
    - O id_profissional NÃO repete.
    - Duas linhas idênticas também recebem IDs diferentes.
    - Cada mês possui uma faixa exclusiva, impedindo colisão entre partições.
    """
    if df.empty:
        return df

    if len(df) >= TAMANHO_BLOCO_ID:
        raise ValueError(
            f"A competência {competencia} possui {len(df):,} linhas. "
            f"O limite por bloco é {TAMANHO_BLOCO_ID - 1:,}. "
            "Aumente TAMANHO_BLOCO_ID e o tamanho da coluna no banco."
        )

    ordenacao = [c for c in colunas_ordenacao if c in df.columns]

    if ordenacao:
        df = (
            df
            .sort_values(
                ordenacao,
                kind="mergesort",
                na_position="last",
            )
            .reset_index(drop=True)
        )
    else:
        df = df.reset_index(drop=True)

    inicio = calcular_inicio_bloco_id(competencia)

    ids = pd.Series(
        range(inicio + 1, inicio + len(df) + 1),
        dtype="int64",
    )

    df.insert(0, nome_id, ids)

    if df[nome_id].duplicated().any():
        raise RuntimeError(f"ERRO: IDs duplicados detectados em '{nome_id}'.")

    print(
        f"🔑 {nome_id}: {len(df):,} IDs únicos criados "
        f"({df[nome_id].min()} até {df[nome_id].max()})."
    )

    return df


# ============================================================
# PROCESSAMENTO RD - INTERNAÇÕES
# ============================================================

def processar_rd(df_rd, competencia, mapa_cid10):
    colunas = [
        "N_AIH",
        "DIAG_PRINC",
        "DT_INTER",
        "DT_SAIDA",
        "MUNIC_RES",
        "SEXO",
        "IDADE",
        "RACA_COR",
        "ETNIA",
        "MES_CMPT",
        "ANO_CMPT",
        "CNES",
        "MARCA_UTI",
        "VAL_TOT",
        "UTI_MES_TO",
        "CAR_INT",
        "MUNIC_MOV",
    ]

    colunas_saida = [
        "N_AIH",
        "DIAG_PRINC",
        "DIAG_DESCRICAO",
        "DT_INTER",
        "DT_SAIDA",
        "MUNIC_RES",
        "SEXO",
        "IDADE",
        "RACA_COR",
        "ETNIA",
        "MES_CMPT",
        "ANO_CMPT",
        "CNES",
        "MARCA_UTI",
        "VAL_TOT",
        "UTI_MES_TO",
        "CAR_INT",
        "MUNIC_MOV",
        "DT_COMPETEN",
    ]

    if df_rd.empty:
        return pd.DataFrame(columns=colunas_saida)

    df = df_rd.copy()

    for c in colunas:
        if c not in df.columns:
            df[c] = None

    df = df[colunas].copy()

    ano_cmpt = padronizar_codigo(df["ANO_CMPT"], 4)
    mes_cmpt = padronizar_codigo(df["MES_CMPT"], 2)

    df["DT_COMPETEN"] = pd.to_datetime(
        ano_cmpt + mes_cmpt + "01",
        format="%Y%m%d",
        errors="coerce",
    )

    df = filtrar_competencia_exata(df, "DT_COMPETEN", competencia)

    if df.empty:
        return pd.DataFrame(columns=colunas_saida)

    df["N_AIH"] = normalizar_codigo(df["N_AIH"])

    # Preserva o código CID em DIAG_PRINC e adiciona a descrição em outra coluna.
    df["DIAG_PRINC"], df["DIAG_DESCRICAO"] = adicionar_descricao_cid10(
        df["DIAG_PRINC"],
        mapa_cid10,
    )

    df["MUNIC_RES"] = padronizar_codigo(df["MUNIC_RES"], 6)
    df["MUNIC_MOV"] = padronizar_codigo(df["MUNIC_MOV"], 6)
    df["CNES"] = padronizar_codigo(df["CNES"], 7)
    df["ETNIA"] = normalizar_codigo(df["ETNIA"])

    df["IDADE"] = pd.to_numeric(df["IDADE"], errors="coerce")
    df["VAL_TOT"] = pd.to_numeric(df["VAL_TOT"], errors="coerce").round(1)
    df["UTI_MES_TO"] = pd.to_numeric(df["UTI_MES_TO"], errors="coerce")

    df["DT_INTER"] = limpar_data_intervalo(
        pd.to_datetime(df["DT_INTER"], format="%Y%m%d", errors="coerce"),
        "DT_INTER",
    )

    df["DT_SAIDA"] = limpar_data_intervalo(
        pd.to_datetime(df["DT_SAIDA"], format="%Y%m%d", errors="coerce"),
        "DT_SAIDA",
    )

    df["DT_COMPETEN"] = limpar_data_intervalo(
        df["DT_COMPETEN"],
        "DT_COMPETEN",
    )

    # SEXO chega no RD como 1 e 3; não completar com zero à esquerda.
    df["SEXO"] = traduzir_codigo(df["SEXO"], MAP_SEXO, "SEXO")

    df["RACA_COR"] = traduzir_codigo(
        df["RACA_COR"], MAP_RACA_COR, "RACA_COR", largura=2
    )
    df["MARCA_UTI"] = traduzir_codigo(
        df["MARCA_UTI"], MAP_MARCA_UTI, "MARCA_UTI", largura=2
    )
    df["CAR_INT"] = traduzir_codigo(
        df["CAR_INT"], MAP_CAR_INT, "CAR_INT", largura=2
    )

    return df[colunas_saida]


# ============================================================
# PROCESSAMENTO ST - CADASTRO
# ============================================================

def processar_st(df_st, competencia):
    colunas = [
        "CNES",
        "TP_UNID",
        "CODUFMUN",
        "REGSAUDE",
        "CLIENTEL",
        "COMPETEN",
        "ALVARA",
        "DT_EXPED",
    ]

    colunas_saida = [
        "CNES",
        "TP_UNID",
        "CODUFMUN",
        "REGSAUDE",
        "CLIENTEL",
        "COMPETEN",
        "alvara",
        "DT_EXPED",
    ]

    if df_st.empty:
        return pd.DataFrame(columns=colunas_saida)

    df = df_st.copy()

    for c in colunas:
        if c not in df.columns:
            df[c] = None

    df = df[colunas].copy()
    df["COMPETEN"] = converter_competencia_para_data(df, "COMPETEN")
    df = filtrar_competencia_exata(df, "COMPETEN", competencia)

    if df.empty:
        return pd.DataFrame(columns=colunas_saida)

    df["CNES"] = padronizar_codigo(df["CNES"], 7)
    df["CODUFMUN"] = padronizar_codigo(df["CODUFMUN"], 6)
    df["REGSAUDE"] = normalizar_codigo(df["REGSAUDE"])

    # Antigo ALVARA passa a se chamar alvara.
    df["alvara"] = normalizar_codigo(df["ALVARA"])
    df.drop(columns=["ALVARA"], inplace=True)

    df["COMPETEN"] = limpar_data_intervalo(df["COMPETEN"], "COMPETEN")
    df["DT_EXPED"] = limpar_data_intervalo(
        pd.to_datetime(df["DT_EXPED"], format="%Y%m%d", errors="coerce"),
        "DT_EXPED",
    )

    df["TP_UNID"] = traduzir_codigo(
        df["TP_UNID"],
        MAP_TP_UNID,
        "TP_UNID",
        largura=2,
    )

    df["CLIENTEL"] = traduzir_codigo(
        df["CLIENTEL"],
        MAP_CLIENTEL,
        "CLIENTEL",
        largura=2,
    )

    return df[colunas_saida]


# ============================================================
# PROCESSAMENTO PF - PROFISSIONAIS
# ============================================================

def processar_pf(df_pf, competencia, mapa_cbo):
    """
    CNS_PROF passa a se chamar cns.

    id_profissional é artificial e identifica a LINHA/REGISTRO, não o CNS.
    Portanto, CNS repetido é permitido, id_profissional repetido não é.
    """
    colunas = [
        "COMPETEN",
        "HORAHOSP",
        "PROF_SUS",
        "CNS_PROF",
        "CBO",
        "REGISTRO",
        "CNES",
        "CODUFMUN",
    ]

    colunas_saida = [
        "id_profissional",
        "cns",
        "COMPETEN",
        "HORAHOSP",
        "PROF_SUS",
        "CBO",
        "REGISTRO",
        "CNES",
        "CODUFMUN",
    ]

    if df_pf.empty:
        return pd.DataFrame(columns=colunas_saida)

    df = df_pf.copy()

    for c in colunas:
        if c not in df.columns:
            df[c] = None

    df = df[colunas].copy()
    df["COMPETEN"] = converter_competencia_para_data(df, "COMPETEN")
    df = filtrar_competencia_exata(df, "COMPETEN", competencia)

    if df.empty:
        return pd.DataFrame(columns=colunas_saida)

    # Mantém a regra já existente de exclusão de famílias CBO.
    # Primeiro trabalhamos apenas com o código de 6 dígitos; a descrição
    # é acrescentada somente depois do filtro.
    df["CBO"] = normalizar_cbo_codigo(df["CBO"])
    prefixos_exclusao = ("51", "41", "42", "25", "78")

    df = df[
        ~df["CBO"].str.startswith(prefixos_exclusao, na=False)
    ].copy()

    if df.empty:
        return pd.DataFrame(columns=colunas_saida)

    df["HORAHOSP"] = pd.to_numeric(df["HORAHOSP"], errors="coerce")
    df["REGISTRO"] = normalizar_codigo(df["REGISTRO"])
    df["CNES"] = padronizar_codigo(df["CNES"], 7)
    df["CODUFMUN"] = padronizar_codigo(df["CODUFMUN"], 6)

    # Antigo CNS_PROF passa a se chamar cns.
    df["cns"] = padronizar_codigo(df["CNS_PROF"], 15)
    df.drop(columns=["CNS_PROF"], inplace=True)

    df["COMPETEN"] = limpar_data_intervalo(df["COMPETEN"], "COMPETEN")
    df["PROF_SUS"] = traduzir_codigo(
        df["PROF_SUS"],
        MAP_SIM_NAO,
        "PROF_SUS",
        largura=2,
    )

    # Sobrescreve o próprio campo CBO no formato:
    #     XXXXXX - Título da ocupação
    # sem criar coluna ou tabela adicional.
    df["CBO"] = traduzir_cbo_no_proprio_campo(
        df["CBO"],
        mapa_cbo,
    )

    # IDs determinísticos por competência e únicos por linha.
    df = adicionar_id_artificial(
        df=df,
        competencia=competencia,
        nome_id="id_profissional",
        colunas_ordenacao=[
            "cns",
            "CNES",
            "CBO",
            "REGISTRO",
            "CODUFMUN",
            "HORAHOSP",
            "PROF_SUS",
        ],
    )

    qtd_cns_repetidos = int(df["cns"].duplicated(keep=False).sum())

    if qtd_cns_repetidos > 0:
        print(
            f"ℹ️ PF {competencia}: {qtd_cns_repetidos:,} linha(s) possuem "
            "CNS repetido. Isso é permitido; id_profissional permanece único."
        )

    return df[colunas_saida]


# ============================================================
# PROCESSAMENTO EQ - EQUIPAMENTOS
# ============================================================

def processar_eq(df_eq, competencia):
    colunas = [
        "CNES",
        "CODUFMUN",
        "TIPEQUIP",
        "CODEQUIP",
        "QT_EXIST",
        "QT_USO",
        "COMPETEN",
        "IND_SUS",
    ]

    colunas_saida = [
        "CNES",
        "CODUFMUN",
        "TIPEQUIP",
        "TIPO_EQUIPAMENTO",
        "CODEQUIP",
        "DESC_EQUIPAMENTO",
        "QT_EXIST",
        "QT_USO",
        "COMPETEN",
        "IND_SUS",
    ]

    if df_eq.empty:
        return pd.DataFrame(columns=colunas_saida)

    df = df_eq.copy()

    for c in colunas:
        if c not in df.columns:
            df[c] = None

    df = df[colunas].copy()
    df["COMPETEN"] = converter_competencia_para_data(df, "COMPETEN")
    df = filtrar_competencia_exata(df, "COMPETEN", competencia)

    if df.empty:
        return pd.DataFrame(columns=colunas_saida)

    df["CNES"] = padronizar_codigo(df["CNES"], 7)
    df["CODUFMUN"] = padronizar_codigo(df["CODUFMUN"], 6)

    # Preserva os códigos vindos do CNES na saída.
    df["TIPEQUIP"] = normalizar_codigo(df["TIPEQUIP"])
    df["CODEQUIP"] = padronizar_codigo(df["CODEQUIP"], 2)

    # O CNES pode trazer TIPEQUIP como "07"/"09", enquanto os domínios
    # semânticos são 7/9. Criamos apenas uma Series auxiliar para as
    # traduções, sem adicionar nova coluna ao Parquet.
    tipo_chave = normalizar_tipequip(df["TIPEQUIP"])

    df["TIPO_EQUIPAMENTO"] = (
        tipo_chave
        .map(MAP_TIPEQUIP)
        .fillna(df["TIPEQUIP"])
        .astype("string")
    )

    # CODEQUIP depende de TIPEQUIP. A tradução é feita por tipo para
    # reduzir o consumo de RAM em tabelas nacionais grandes.
    df["DESC_EQUIPAMENTO"] = pd.Series(
        pd.NA,
        index=df.index,
        dtype="string",
    )

    for tipo in tipo_chave.dropna().unique():
        mapa_tipo = MAP_CODEQUIP_POR_TIPO.get(str(tipo))

        if not mapa_tipo:
            continue

        mascara_tipo = tipo_chave == tipo

        df.loc[mascara_tipo, "DESC_EQUIPAMENTO"] = (
            df.loc[mascara_tipo, "CODEQUIP"]
            .map(mapa_tipo)
            .astype("string")
        )

    sem_traducao = (
        tipo_chave.notna()
        & df["CODEQUIP"].notna()
        & df["DESC_EQUIPAMENTO"].isna()
    )

    if sem_traducao.any():
        exemplos = (
            df.loc[
                sem_traducao,
                ["TIPEQUIP", "CODEQUIP"],
            ]
            .drop_duplicates()
            .head(10)
            .astype("string")
            .agg("/".join, axis=1)
            .tolist()
        )

        print(
            f"ℹ️ 'TIPEQUIP/CODEQUIP': {int(sem_traducao.sum()):,} "
            "registro(s) sem descrição cadastrada. "
            f"Exemplos: {exemplos}"
        )

    df["QT_EXIST"] = pd.to_numeric(df["QT_EXIST"], errors="coerce")
    df["QT_USO"] = pd.to_numeric(df["QT_USO"], errors="coerce")
    df["COMPETEN"] = limpar_data_intervalo(df["COMPETEN"], "COMPETEN")

    df["IND_SUS"] = traduzir_codigo(
        df["IND_SUS"],
        MAP_SIM_NAO,
        "IND_SUS",
        largura=2,
    )

    return df[colunas_saida]


# ============================================================
# PROCESSAMENTO LT - LEITOS
# ============================================================

def processar_lt(df_lt, competencia):
    """
    Processa os leitos do CNES preservando a granularidade do CODLEITO.

    O campo CODLEITO continua sendo uma única coluna, porém passa a sair como:
        XX - Descrição do leito

    Exemplos:
        02 - Cardiologia
        32 - Cardiologia
        12 - Oncologia
        44 - Oncologia

    Isso evita que códigos diferentes que possuem a mesma descrição sejam
    colapsados visualmente na Silver/Gold.
    """
    colunas = [
        "CNES",
        "CODUFMUN",
        "CODLEITO",
        "QT_CONTR",
        "QT_SUS",
        "QT_EXIST",
        "COMPETEN",
    ]

    if df_lt.empty:
        return pd.DataFrame(columns=colunas)

    df = df_lt.copy()

    for c in colunas:
        if c not in df.columns:
            df[c] = None

    df = df[colunas].copy()
    df["COMPETEN"] = converter_competencia_para_data(df, "COMPETEN")
    df = filtrar_competencia_exata(df, "COMPETEN", competencia)

    if df.empty:
        return pd.DataFrame(columns=colunas)

    df["CNES"] = padronizar_codigo(df["CNES"], 7)
    df["CODUFMUN"] = padronizar_codigo(df["CODUFMUN"], 6)
    df["QT_CONTR"] = pd.to_numeric(df["QT_CONTR"], errors="coerce")
    df["QT_SUS"] = pd.to_numeric(df["QT_SUS"], errors="coerce")
    df["QT_EXIST"] = pd.to_numeric(df["QT_EXIST"], errors="coerce")
    df["COMPETEN"] = limpar_data_intervalo(df["COMPETEN"], "COMPETEN")

    # Mantém o código original e adiciona a descrição no próprio campo.
    # Isso diferencia, por exemplo, 02 - Cardiologia de 32 - Cardiologia.
    codigos = padronizar_codigo(df["CODLEITO"], 2)
    descricoes = codigos.map(MAP_CODLEITO).astype("string")

    tipo_leito = codigos.copy()
    encontrados = codigos.notna() & descricoes.notna()

    tipo_leito.loc[encontrados] = (
        codigos.loc[encontrados]
        + " - "
        + descricoes.loc[encontrados]
    )

    sem_traducao = codigos.notna() & descricoes.isna()

    if sem_traducao.any():
        exemplos = (
            codigos.loc[sem_traducao]
            .drop_duplicates()
            .sort_values()
            .head(10)
            .tolist()
        )

        print(
            f"ℹ️ 'CODLEITO': {int(sem_traducao.sum()):,} registro(s) "
            "sem descrição cadastrada; o código original foi preservado. "
            f"Exemplos: {exemplos}"
        )

    df["CODLEITO"] = tipo_leito.astype("string")

    # Diagnóstico do grão esperado na Silver. Não remove nenhuma linha.
    duplicados_grao = df.duplicated(
        subset=["CNES", "COMPETEN", "CODLEITO"],
        keep=False,
    )

    qtd_duplicados_grao = int(duplicados_grao.sum())

    if qtd_duplicados_grao > 0:
        print(
            f"⚠️ LT {competencia}: {qtd_duplicados_grao:,} linha(s) participam "
            "de combinações repetidas CNES + COMPETEN + CODLEITO. "
            "Nenhuma linha foi removida."
        )

    return df


# ============================================================
# DIMENSÃO ALVARÁ
# ============================================================

def gerar_dim_alvara_competencia(df_st, competencia):
    """
    Gera registros de Alvará para uma competência.

    alvara = antigo ALVARA.
    id_alvara = artificial e único por registro.
    """
    colunas_saida = [
        "id_alvara",
        "alvara",
        "data_emissao",
        "CNES_UNIDADE_VER",
    ]

    if df_st.empty or "alvara" not in df_st.columns:
        return pd.DataFrame(columns=colunas_saida)

    df = df_st[
        ["alvara", "DT_EXPED", "COMPETEN", "CNES"]
    ].copy()

    invalidos = {
        "",
        "None",
        "nan",
        "NaN",
        "0",
        "00",
        "NoneType",
        "<NA>",
    }

    df["alvara"] = normalizar_codigo(df["alvara"])

    df = df[
        df["alvara"].notna()
        & ~df["alvara"].isin(invalidos)
    ].copy()

    if df.empty:
        return pd.DataFrame(columns=colunas_saida)

    df["data_emissao"] = limpar_data_intervalo(
        df["DT_EXPED"],
        "data_emissao",
    )

    comp_str = df["COMPETEN"].dt.strftime("%Y%m")

    df["CNES_UNIDADE_VER"] = (
        comp_str
        + padronizar_codigo(df["CNES"], 7)
    )

    df = (
        df[
            ["alvara", "data_emissao", "CNES_UNIDADE_VER"]
        ]
        .drop_duplicates()
        .sort_values(
            ["CNES_UNIDADE_VER", "alvara", "data_emissao"],
            kind="mergesort",
            na_position="last",
        )
        .reset_index(drop=True)
    )

    df = adicionar_id_artificial(
        df=df,
        competencia=competencia,
        nome_id="id_alvara",
        colunas_ordenacao=[
            "CNES_UNIDADE_VER",
            "alvara",
            "data_emissao",
        ],
    )

    return df[colunas_saida]


# ============================================================
# DIMENSÃO DATA
# ============================================================

def adicionar_datas_ao_set(destino, serie):
    if serie is None or not isinstance(serie, pd.Series) or serie.empty:
        return

    serie = limpar_data_intervalo(serie)

    for valor in serie.dropna().unique():
        destino.add(pd.Timestamp(valor))


def gerar_dim_data(datas_unicas):
    if not datas_unicas:
        return pd.DataFrame()

    datas = sorted(
        data
        for data in datas_unicas
        if DATA_MIN <= data <= DATA_MAX
    )

    if not datas:
        return pd.DataFrame()

    df = pd.DataFrame({"data": pd.to_datetime(datas)})
    d = df["data"]

    df["mes"] = d.dt.strftime("%m")
    df["ano"] = d.dt.strftime("%Y")
    df["dia_ano"] = d.dt.dayofyear.astype(str)
    df["dia_mes"] = d.dt.day.astype(str)
    df["semana_mes"] = ((d.dt.day - 1) // 7 + 1).astype(str)
    df["semana_ano"] = d.dt.isocalendar().week.astype(str)

    dias_pt = {
        0: "Segunda-feira",
        1: "Terça-feira",
        2: "Quarta-feira",
        3: "Quinta-feira",
        4: "Sexta-feira",
        5: "Sábado",
        6: "Domingo",
    }

    meses_pt = {
        1: "Janeiro",
        2: "Fevereiro",
        3: "Março",
        4: "Abril",
        5: "Maio",
        6: "Junho",
        7: "Julho",
        8: "Agosto",
        9: "Setembro",
        10: "Outubro",
        11: "Novembro",
        12: "Dezembro",
    }

    df["dia_semana_nome"] = d.dt.dayofweek.map(dias_pt)
    df["mes_nome"] = d.dt.month.map(meses_pt)
    df["bimestre"] = ((d.dt.month - 1) // 2 + 1).astype(str)
    df["trimestre"] = d.dt.quarter.astype(str)
    df["semestre"] = ((d.dt.month - 1) // 6 + 1).astype(str)

    return df


# ============================================================
# DIMENSÃO MUNICÍPIO
# ============================================================

def adicionar_municipios_ao_set(destino, serie):
    if serie is None or not isinstance(serie, pd.Series) or serie.empty:
        return

    codigos = padronizar_codigo(serie, 6)

    for codigo in codigos.dropna().unique():
        destino.add(str(codigo))


def gerar_dim_municipio(codigos_municipio):
    if not codigos_municipio:
        return pd.DataFrame(
            columns=["id_municipio", "nome_municipio", "regiao", "uf"]
        )

    valid_cods = sorted(codigos_municipio)
    df_m = pd.DataFrame({"id_municipio": valid_cods})

    print("🌐 Buscando catálogo oficial de municípios na API do IBGE...")

    try:
        url = "https://servicodados.ibge.gov.br/api/v1/localidades/municipios"
        res = requests.get(url, timeout=30, verify=False)
        res.raise_for_status()

        df_ibge = pd.DataFrame(
            [
                {
                    "id_municipio": str(item["id"])[:6],
                    "nome_municipio": item["nome"],
                }
                for item in res.json()
            ]
        ).drop_duplicates(subset=["id_municipio"])

        df_m = df_m.merge(
            df_ibge,
            on="id_municipio",
            how="left",
        )

    except Exception as e:
        print(f"⚠️ Erro ao consultar API do IBGE: {e}")
        df_m["nome_municipio"] = "Não Informado"

    df_m["nome_municipio"] = (
        df_m["nome_municipio"]
        .fillna("Não Informado")
        .replace({"nan": "Não Informado", "": "Não Informado"})
    )

    uf_map = {
        "11": "RO", "12": "AC", "13": "AM", "14": "RR", "15": "PA",
        "16": "AP", "17": "TO", "21": "MA", "22": "PI", "23": "CE",
        "24": "RN", "25": "PB", "26": "PE", "27": "AL", "28": "SE",
        "29": "BA", "31": "MG", "32": "ES", "33": "RJ", "35": "SP",
        "41": "PR", "42": "SC", "43": "RS", "50": "MS", "51": "MT",
        "52": "GO", "53": "DF",
    }

    regiao_map = {
        "11": "Norte", "12": "Norte", "13": "Norte", "14": "Norte",
        "15": "Norte", "16": "Norte", "17": "Norte",
        "21": "Nordeste", "22": "Nordeste", "23": "Nordeste",
        "24": "Nordeste", "25": "Nordeste", "26": "Nordeste",
        "27": "Nordeste", "28": "Nordeste", "29": "Nordeste",
        "31": "Sudeste", "32": "Sudeste", "33": "Sudeste", "35": "Sudeste",
        "41": "Sul", "42": "Sul", "43": "Sul",
        "50": "Centro-Oeste", "51": "Centro-Oeste",
        "52": "Centro-Oeste", "53": "Centro-Oeste",
    }

    prefixo_uf = df_m["id_municipio"].str[:2]
    df_m["uf"] = prefixo_uf.map(uf_map).fillna("EX")
    df_m["regiao"] = prefixo_uf.map(regiao_map).fillna("Exterior/Outros")

    return df_m[
        ["id_municipio", "nome_municipio", "regiao", "uf"]
    ]


# ============================================================
# VALIDAÇÕES
# ============================================================

def validar_pk(df, coluna, nome_tabela):
    if df.empty:
        return

    if coluna not in df.columns:
        raise RuntimeError(
            f"Tabela '{nome_tabela}' não possui a coluna PK '{coluna}'."
        )

    nulos = int(df[coluna].isna().sum())
    duplicados = int(df[coluna].duplicated().sum())

    if nulos > 0 or duplicados > 0:
        raise RuntimeError(
            f"Falha de integridade em '{nome_tabela}.{coluna}': "
            f"{nulos:,} nulo(s), {duplicados:,} duplicado(s)."
        )

    print(
        f"✅ PK validada: {nome_tabela}.{coluna} "
        f"({len(df):,} valores únicos)."
    )


# ============================================================
# EXECUÇÃO PRINCIPAL
# ============================================================

def executar_pipeline_silver():
    client, namespace = get_oci_client()

    # Carrega os catálogos de referência uma única vez por execução.
    mapa_cid10 = carregar_mapa_cid10()
    mapa_cbo = carregar_mapa_cbo()

    competencias = obter_ultimos_12_meses_completos()

    print("\n=======================================================")
    print("🥈 PIPELINE SILVER - SYNTHeSUS | AIRFLOW / DOCKER")
    print("=======================================================")
    print("Competências:")

    for comp in competencias:
        print(f"  → {comp[4:6]}/{comp[:4]}")

    print(f"Total: {len(competencias)} competências")

    # Lista o bucket Bronze apenas uma vez.
    nomes_bronze = listar_nomes_objetos(
        client,
        namespace,
        BUCKET_BRONZE,
    )

    # Acumuladores leves para dimensões globais.
    datas_dim = set()
    municipios_dim = set()
    partes_dim_alvara = []

    # Cada fonte é processada, salva e liberada antes da próxima.
    # Isso reduz o pico de RAM comparado a manter RD/ST/PF/EQ/LT
    # simultaneamente em memória.
    for competencia in competencias:
        print("\n=======================================================")
        print(
            f"📅 PROCESSANDO SILVER: "
            f"{competencia[4:6]}/{competencia[:4]}"
        )
        print("=======================================================")

        # ====================================================
        # 1. RD - INTERNAÇÕES
        # ====================================================
        print("\n🏥 RD - Internações")
        df_raw = ler_bronze_competencia(
            client, namespace, BUCKET_BRONZE,
            nomes_bronze, "raw_sih_rd", competencia
        )
        df = processar_rd(df_raw, competencia, mapa_cid10)

        adicionar_datas_ao_set(datas_dim, df.get("DT_INTER"))
        adicionar_datas_ao_set(datas_dim, df.get("DT_SAIDA"))
        adicionar_datas_ao_set(datas_dim, df.get("DT_COMPETEN"))
        adicionar_municipios_ao_set(municipios_dim, df.get("MUNIC_RES"))
        adicionar_municipios_ao_set(municipios_dim, df.get("MUNIC_MOV"))

        salvar_parquet_oci(
            client, namespace, BUCKET_SILVER,
            f"silver_internacoes_rd_{competencia}.parquet",
            df,
        )
        del df_raw, df
        gc.collect()

        # ====================================================
        # 2. ST - CADASTRO
        # ====================================================
        print("\n🏢 ST - Cadastro")
        df_raw = ler_bronze_competencia(
            client, namespace, BUCKET_BRONZE,
            nomes_bronze, "raw_cnes_st", competencia
        )
        df = processar_st(df_raw, competencia)

        adicionar_datas_ao_set(datas_dim, df.get("COMPETEN"))
        adicionar_datas_ao_set(datas_dim, df.get("DT_EXPED"))
        adicionar_municipios_ao_set(municipios_dim, df.get("CODUFMUN"))

        dim_alvara_mes = gerar_dim_alvara_competencia(df, competencia)
        validar_pk(
            dim_alvara_mes,
            "id_alvara",
            f"dim_alvara_{competencia}",
        )

        if not dim_alvara_mes.empty:
            partes_dim_alvara.append(dim_alvara_mes)

        salvar_parquet_oci(
            client, namespace, BUCKET_SILVER,
            f"silver_cadastro_st_{competencia}.parquet",
            df,
        )
        del df_raw, df, dim_alvara_mes
        gc.collect()

        # ====================================================
        # 3. PF - PROFISSIONAIS
        # ====================================================
        print("\n👩‍⚕️ PF - Profissionais")
        df_raw = ler_bronze_competencia(
            client, namespace, BUCKET_BRONZE,
            nomes_bronze, "raw_cnes_pf", competencia
        )
        df = processar_pf(df_raw, competencia, mapa_cbo)

        validar_pk(
            df,
            "id_profissional",
            f"silver_profissionais_pf_{competencia}",
        )

        adicionar_datas_ao_set(datas_dim, df.get("COMPETEN"))
        adicionar_municipios_ao_set(municipios_dim, df.get("CODUFMUN"))

        salvar_parquet_oci(
            client, namespace, BUCKET_SILVER,
            f"silver_profissionais_pf_{competencia}.parquet",
            df,
        )
        del df_raw, df
        gc.collect()

        # ====================================================
        # 4. EQ - EQUIPAMENTOS
        # ====================================================
        print("\n🩺 EQ - Equipamentos")
        df_raw = ler_bronze_competencia(
            client, namespace, BUCKET_BRONZE,
            nomes_bronze, "raw_cnes_eq", competencia
        )
        df = processar_eq(df_raw, competencia)

        adicionar_datas_ao_set(datas_dim, df.get("COMPETEN"))
        adicionar_municipios_ao_set(municipios_dim, df.get("CODUFMUN"))

        salvar_parquet_oci(
            client, namespace, BUCKET_SILVER,
            f"silver_equipamentos_eq_{competencia}.parquet",
            df,
        )
        del df_raw, df
        gc.collect()

        # ====================================================
        # 5. LT - LEITOS
        # ====================================================
        print("\n🛏️ LT - Leitos")
        df_raw = ler_bronze_competencia(
            client, namespace, BUCKET_BRONZE,
            nomes_bronze, "raw_cnes_lt", competencia
        )
        df = processar_lt(df_raw, competencia)

        adicionar_datas_ao_set(datas_dim, df.get("COMPETEN"))
        adicionar_municipios_ao_set(municipios_dim, df.get("CODUFMUN"))

        salvar_parquet_oci(
            client, namespace, BUCKET_SILVER,
            f"silver_leitos_lt_{competencia}.parquet",
            df,
        )
        del df_raw, df
        gc.collect()

        print(f"\n✅ Competência {competencia} concluída e memória liberada.")

    # ========================================================
    # DIMENSÕES GLOBAIS
    # ========================================================
    print("\n=======================================================")
    print("📐 CONSTRUINDO DIMENSÕES GLOBAIS")
    print("=======================================================")

    dim_data = gerar_dim_data(datas_dim)
    dim_municipio = gerar_dim_municipio(municipios_dim)

    if partes_dim_alvara:
        dim_alvara = (
            pd.concat(partes_dim_alvara, ignore_index=True)
            .sort_values("id_alvara")
            .reset_index(drop=True)
        )
    else:
        dim_alvara = pd.DataFrame(
            columns=[
                "id_alvara",
                "alvara",
                "data_emissao",
                "CNES_UNIDADE_VER",
            ]
        )

    validar_pk(dim_alvara, "id_alvara", "dim_alvara")

    salvar_parquet_oci(
        client, namespace, BUCKET_SILVER,
        "dim_data.parquet",
        dim_data,
    )

    salvar_parquet_oci(
        client, namespace, BUCKET_SILVER,
        "dim_municipio.parquet",
        dim_municipio,
    )

    salvar_parquet_oci(
        client, namespace, BUCKET_SILVER,
        "dim_alvara.parquet",
        dim_alvara,
    )

    print("\n=======================================================")
    print("✅ PIPELINE SILVER CONCLUÍDO NO AIRFLOW / DOCKER")
    print("=======================================================")
    print("✓ Últimos 12 meses completos processados.")
    print("✓ Cada fonte foi liberada da RAM antes da próxima.")
    print("✓ Silver particionada por competência.")
    print("✓ id_profissional artificial e sem repetição.")
    print("✓ CNS preservado em 'cns' e pode se repetir.")
    print("✓ id_alvara artificial e sem repetição.")
    print("✓ ALVARA preservado em 'alvara'.")
    print("✓ Datas fora de 1910-2028 transformadas em NaT.")
    print("✓ DIAG_PRINC preservado e DIAG_DESCRICAO preenchida via CID-10 oficial do DATASUS (ZIP local).")
    print("✓ TIPEQUIP preservado e legendas de tipo/equipamento corrigidas.")
    print("✓ CODLEITO preserva código + descrição no formato 'XX - Descrição'.")
    print("✓ CBO preenchido no próprio campo como 'XXXXXX - Título' via base oficial do MTE.")
    print("✓ Processamento fonte-a-fonte reduz o pico de memória do container.")


if __name__ == "__main__":
    executar_pipeline_silver()