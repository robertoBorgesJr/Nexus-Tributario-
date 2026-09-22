"""
Configurações centrais: empresas fictícias, produtos, clientes e períodos.
Todos os geradores importam daqui para garantir consistência entre NF-e, SPED e EFD.
"""
from datetime import date

# ─── Períodos de geração (6 meses) ───────────────────────────────────────────
PERIODOS = [
    date(2025, 7, 1),
    date(2025, 8, 1),
    date(2025, 9, 1),
    date(2025, 10, 1),
    date(2025, 11, 1),
    date(2025, 12, 1),
]

# ─── Empresas emitentes ───────────────────────────────────────────────────────
EMPRESAS = [
    {
        "cnpj": "11222333000181",
        "razao_social": "INDUSTRIA PAULISTA DE ELETRONICOS LTDA",
        "nome_fantasia": "PAULITECH",
        "ie": "111222333444",
        "uf": "SP",
        "cuf": 35,
        "municipio": "SAO PAULO",
        "cmun": "3550308",
        "cep": "01310100",
        "logradouro": "AV PAULISTA",
        "numero": "1000",
        "bairro": "BELA VISTA",
        "crt": "3",
        "regime": "LUCRO_REAL",
        "cnae": "2610800",
    },
    {
        "cnpj": "22333444000195",
        "razao_social": "COMERCIO CARIOCA DE INFORMATICA SA",
        "nome_fantasia": "CARIOCA INFO",
        "ie": "222333444555",
        "uf": "RJ",
        "cuf": 33,
        "municipio": "RIO DE JANEIRO",
        "cmun": "3304557",
        "cep": "20040020",
        "logradouro": "AV RIO BRANCO",
        "numero": "500",
        "bairro": "CENTRO",
        "crt": "3",
        "regime": "LUCRO_PRESUMIDO",
        "cnae": "4751200",
    },
    {
        "cnpj": "33444555000108",
        "razao_social": "DISTRIBUIDORA MINEIRA DE INSUMOS EIRELI",
        "nome_fantasia": "MINAS DIST",
        "ie": "3334445560000",
        "uf": "MG",
        "cuf": 31,
        "municipio": "BELO HORIZONTE",
        "cmun": "3106200",
        "cep": "30130110",
        "logradouro": "AV AFONSO PENA",
        "numero": "200",
        "bairro": "CENTRO",
        "crt": "1",
        "regime": "SIMPLES_NACIONAL",
        "cnae": "4647801",
    },
]

# ─── Produtos ─────────────────────────────────────────────────────────────────
PRODUTOS = [
    {
        "cprod": "PROD001",
        "xprod": "NOTEBOOK PROCESSADOR I5 16GB RAM 512GB SSD",
        "ncm": "84713012",
        "cean": "7891234560001",
        "ucom": "UN",
        "valor_unitario": 3500.00,
        "ipi_aliq": 5.0,
        "cst_icms": "00",
        "cst_pis_cofins_saida": "01",
        "cst_pis_cofins_entrada": "50",
    },
    {
        "cprod": "PROD002",
        "xprod": "TABLET 10 POLEGADAS 64GB WIFI",
        "ncm": "84713019",
        "cean": "7891234560002",
        "ucom": "UN",
        "valor_unitario": 1800.00,
        "ipi_aliq": 5.0,
        "cst_icms": "00",
        "cst_pis_cofins_saida": "01",
        "cst_pis_cofins_entrada": "50",
    },
    {
        "cprod": "PROD003",
        "xprod": "MONITOR LED 27 POLEGADAS FULL HD",
        "ncm": "85285210",
        "cean": "7891234560003",
        "ucom": "UN",
        "valor_unitario": 1200.00,
        "ipi_aliq": 10.0,
        "cst_icms": "00",
        "cst_pis_cofins_saida": "01",
        "cst_pis_cofins_entrada": "50",
    },
    {
        "cprod": "PROD004",
        "xprod": "TECLADO MECANICO USB ABNT2",
        "ncm": "84716090",
        "cean": "7891234560004",
        "ucom": "UN",
        "valor_unitario": 350.00,
        "ipi_aliq": 5.0,
        "cst_icms": "00",
        "cst_pis_cofins_saida": "01",
        "cst_pis_cofins_entrada": "50",
    },
    {
        "cprod": "PROD005",
        "xprod": "MOUSE SEM FIO BLUETOOTH",
        "ncm": "84716090",
        "cean": "7891234560005",
        "ucom": "UN",
        "valor_unitario": 180.00,
        "ipi_aliq": 5.0,
        "cst_icms": "00",
        "cst_pis_cofins_saida": "01",
        "cst_pis_cofins_entrada": "50",
    },
    {
        "cprod": "PROD006",
        "xprod": "CABO HDMI 2M",
        "ncm": "85444299",
        "cean": "7891234560006",
        "ucom": "UN",
        "valor_unitario": 45.00,
        "ipi_aliq": 0.0,
        "cst_icms": "00",
        "cst_pis_cofins_saida": "01",
        "cst_pis_cofins_entrada": "50",
    },
    {
        "cprod": "PROD007",
        "xprod": "HD EXTERNO 2TB USB 3.0",
        "ncm": "84717090",
        "cean": "7891234560007",
        "ucom": "UN",
        "valor_unitario": 650.00,
        "ipi_aliq": 5.0,
        "cst_icms": "00",
        "cst_pis_cofins_saida": "01",
        "cst_pis_cofins_entrada": "50",
    },
    {
        "cprod": "PROD008",
        "xprod": "CARREGADOR PORTATIL 20000MAH",
        "ncm": "85044010",
        "cean": "7891234560008",
        "ucom": "UN",
        "valor_unitario": 250.00,
        "ipi_aliq": 0.0,
        "cst_icms": "00",
        "cst_pis_cofins_saida": "01",
        "cst_pis_cofins_entrada": "50",
    },
]

# ─── Clientes (destinatários) ─────────────────────────────────────────────────
CLIENTES = [
    {
        "cnpj": "44555666000117",
        "xnome": "TECH SOLUTIONS COMERCIO E SERVICOS LTDA",
        "uf": "SP",
        "cuf": 35,
        "municipio": "CAMPINAS",
        "cmun": "3509502",
        "cep": "13010050",
        "logradouro": "RUA BARAO DE JAGUARA",
        "numero": "300",
        "bairro": "CENTRO",
        "ie": "444555666777",
    },
    {
        "cnpj": "55666777000130",
        "xnome": "DIGITAL STORE COMERCIAL EIRELI",
        "uf": "PR",
        "cuf": 41,
        "municipio": "CURITIBA",
        "cmun": "4106902",
        "cep": "80020080",
        "logradouro": "RUA XV DE NOVEMBRO",
        "numero": "700",
        "bairro": "CENTRO",
        "ie": "555666777888",
    },
    {
        "cnpj": "66777888000143",
        "xnome": "MEGA INFORMATICA E TELEFONIA LTDA",
        "uf": "RS",
        "cuf": 43,
        "municipio": "PORTO ALEGRE",
        "cmun": "4314902",
        "cep": "90010080",
        "logradouro": "AV BORGES DE MEDEIROS",
        "numero": "1500",
        "bairro": "PRAIA DE BELAS",
        "ie": "666777888999",
    },
    {
        "cnpj": "77888999000156",
        "xnome": "NORTE ELETRONICOS COMERCIO SA",
        "uf": "AM",
        "cuf": 13,
        "municipio": "MANAUS",
        "cmun": "1302603",
        "cep": "69010060",
        "logradouro": "AV SETE DE SETEMBRO",
        "numero": "400",
        "bairro": "CENTRO",
        "ie": "777888999000",
    },
]

# ─── Parâmetros de geração ────────────────────────────────────────────────────
NF_POR_MES = 30
SEED_ALEATORIA = 42

# ─── Alíquotas ICMS ───────────────────────────────────────────────────────────
ALIQ_ICMS_INTRA = {"SP": 12.0, "RJ": 20.0, "MG": 12.0, "RS": 12.0, "PR": 12.0, "AM": 12.0}

ALIQ_ICMS_INTER = {
    ("SP", "RJ"): 12.0, ("SP", "MG"): 12.0, ("SP", "RS"): 7.0,
    ("SP", "PR"): 7.0,  ("SP", "AM"): 12.0,
    ("RJ", "SP"): 12.0, ("RJ", "MG"): 12.0, ("RJ", "RS"): 7.0,
    ("RJ", "PR"): 7.0,  ("RJ", "AM"): 12.0,
    ("MG", "SP"): 12.0, ("MG", "RJ"): 12.0, ("MG", "RS"): 7.0,
    ("MG", "PR"): 7.0,  ("MG", "AM"): 12.0,
}

# ─── Alíquotas PIS/COFINS (Lucro Real — regime não-cumulativo) ────────────────
PIS_ALIQ = 1.65
COFINS_ALIQ = 7.6
