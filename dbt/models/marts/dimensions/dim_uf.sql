{{ config(materialized='table') }}

-- Dimensão estática: não depende de sources, usa seed ou valores fixos
select * from (
    values
        ('AC', 12, 'ACRE',                'Norte'),
        ('AL', 27, 'ALAGOAS',             'Nordeste'),
        ('AM', 13, 'AMAZONAS',            'Norte'),
        ('AP', 16, 'AMAPA',               'Norte'),
        ('BA', 29, 'BAHIA',               'Nordeste'),
        ('CE', 23, 'CEARA',               'Nordeste'),
        ('DF', 53, 'DISTRITO FEDERAL',    'Centro-Oeste'),
        ('ES', 32, 'ESPIRITO SANTO',      'Sudeste'),
        ('GO', 52, 'GOIAS',               'Centro-Oeste'),
        ('MA', 21, 'MARANHAO',            'Nordeste'),
        ('MG', 31, 'MINAS GERAIS',        'Sudeste'),
        ('MS', 50, 'MATO GROSSO DO SUL',  'Centro-Oeste'),
        ('MT', 51, 'MATO GROSSO',         'Centro-Oeste'),
        ('PA', 15, 'PARA',                'Norte'),
        ('PB', 25, 'PARAIBA',             'Nordeste'),
        ('PE', 26, 'PERNAMBUCO',          'Nordeste'),
        ('PI', 22, 'PIAUI',               'Nordeste'),
        ('PR', 41, 'PARANA',              'Sul'),
        ('RJ', 33, 'RIO DE JANEIRO',      'Sudeste'),
        ('RN', 24, 'RIO GRANDE DO NORTE', 'Nordeste'),
        ('RO', 11, 'RONDONIA',            'Norte'),
        ('RR', 14, 'RORAIMA',             'Norte'),
        ('RS', 43, 'RIO GRANDE DO SUL',   'Sul'),
        ('SC', 42, 'SANTA CATARINA',      'Sul'),
        ('SE', 28, 'SERGIPE',             'Nordeste'),
        ('SP', 35, 'SAO PAULO',           'Sudeste'),
        ('TO', 17, 'TOCANTINS',           'Norte')
) as t(uf, cuf, nome_uf, regiao)
