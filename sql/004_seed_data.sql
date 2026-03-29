-- ─────────────────────────────────────────────────────────────────────────────
-- 004_seed_data.sql
-- Seed data for abbreviation_dictionary, exact_mapping, and custom_rules.
-- Safe to run multiple times — uses INSERT OR IGNORE pattern via setup.py.
-- ─────────────────────────────────────────────────────────────────────────────

-- ── Abbreviation dictionary ───────────────────────────────────────────────────
-- Common ag-industry abbreviations found in machine application records.

INSERT INTO product_normalization.abbreviation_dictionary
    (abbreviation, expansion, notes)
VALUES
    -- Herbicides
    ('RU',     'Roundup',               'Glyphosate brand shorthand'),
    ('RUP',    'Roundup PowerMAX',      NULL),
    ('RUPM',   'Roundup PowerMAX',      NULL),
    ('RUPM3',  'Roundup PowerMAX 3',    NULL),
    ('GLYPH',  'Glyphosate',            'Generic active ingredient'),
    ('2,4D',   '2,4-D',                 'Common 2,4-D shorthand'),
    ('24D',    '2,4-D',                 NULL),
    ('DICAM',  'Dicamba',               NULL),
    ('DIC',    'Dicamba',               NULL),
    ('ENGN',   'Engenia',               'Dicamba product'),
    ('XTD',    'Xtendimax',             'Dicamba product'),
    ('XTND',   'Xtendimax',             NULL),
    ('FMCX',   'Tavium',                'FMC dicamba product'),
    ('ATR',    'Atrazine',              NULL),
    ('ATRA',   'Atrazine',              NULL),
    ('ATRZ',   'Atrazine',              NULL),
    ('METR',   'Metribuzin',            NULL),
    ('HAR',    'Harness',               'Acetochlor herbicide'),
    ('HARX',   'Harness Xtra',          NULL),
    ('DUAL',   'Dual Magnum',           'S-metolachlor'),
    ('DUALM',  'Dual Magnum',           NULL),
    ('BKTL',   'Bucktril',              'Bromoxynil herbicide'),
    ('CLO',    'Clarity',               'Dicamba product'),
    ('BSGRM',  'Basagran',              NULL),
    ('PREF',   'Prefix',                'Metolachlor + fomesafen'),
    ('FLX',    'Flexstar',              'Fomesafen'),
    ('FLXGT',  'Flexstar GT',           NULL),
    ('CONV',   'Converge',              NULL),
    ('CONF',   'Confidence',            NULL),
    ('LBER',   'Liberty',               'Glufosinate'),
    ('LIB',    'Liberty',               NULL),
    ('STATUS', 'Status',                'Dicamba + diflufenzopyr'),

    -- Fungicides
    ('PRO',    'Proline',               NULL),
    ('PROL',   'Proline',               NULL),
    ('QUER',   'Quilt Xcel',            NULL),
    ('QXCEL',  'Quilt Xcel',            NULL),
    ('HEAD',   'Headline',              'Pyraclostrobin fungicide'),
    ('HDLN',   'Headline',              NULL),
    ('HEADAMP','Headline AMP',          NULL),
    ('HDLNAMP','Headline AMP',          NULL),
    ('PRIST',  'Priaxor',               NULL),
    ('PRIX',   'Priaxor',               NULL),
    ('EVTL',   'Evito',                 NULL),
    ('TILT',   'Tilt',                  'Propiconazole'),
    ('PROPI',  'Propiconazole',         'Generic active ingredient'),
    ('COBRA',  'Cobra',                 'Lactofen fungicide'),
    ('APROACH','Aproach Prima',         NULL),
    ('APRP',   'Aproach Prima',         NULL),
    ('MIRA',   'Miravis Neo',           NULL),
    ('MNEO',   'Miravis Neo',           NULL),
    ('DELARO', 'Delaro Complete',       NULL),
    ('DLRO',   'Delaro',               NULL),

    -- Insecticides
    ('BAYTHD', 'Baythroid XL',          NULL),
    ('BYXL',   'Baythroid XL',          NULL),
    ('KARATE', 'Karate Z',              NULL),
    ('KARZ',   'Karate Z',              NULL),
    ('PYRHD',  'Warrior II',            NULL),
    ('LSPRY',  'LambdaCyhalothrin',     'Generic lambda-cy'),
    ('MALATH', 'Malathion',             NULL),
    ('CHLO',   'Chlorpyrifos',          NULL),
    ('CLPF',   'Chlorpyrifos',          NULL),
    ('DECI',   'Decis',                 'Deltamethrin'),
    ('ASSAIL', 'Assail 30SG',           NULL),
    ('ASSL',   'Assail',                NULL),
    ('STEWARD','Steward',               'Indoxacarb insecticide'),
    ('STWD',   'Steward',               NULL),
    ('BELEM',  'Belt',                  'Flubendiamide'),

    -- Fertilizers
    ('UAN',    'UAN 28-0-0',            '28% liquid nitrogen'),
    ('UAN28',  'UAN 28-0-0',            NULL),
    ('UAN32',  'UAN 32-0-0',            NULL),
    ('AMS',    'Ammonium Sulfate',      '21-0-0-24S'),
    ('ACS',    'Ammonium Sulfate',      NULL),
    ('MAP',    'MAP 11-52-0',           'Monoammonium phosphate'),
    ('DAP',    'DAP 18-46-0',           'Diammonium phosphate'),
    ('POT',    'Potash 0-0-60',         'Muriate of potash'),
    ('MOP',    'Potash 0-0-60',         'Muriate of potash'),
    ('UREA',   'Urea 46-0-0',           NULL),
    ('SULP',   'Sulfur',                NULL),
    ('ZINC',   'Zinc Sulfate',          NULL),
    ('BORON',  'Boron',                 NULL),
    ('MICRO',  'Micronutrient Package', NULL),

    -- Biologicals
    ('LRSI',   'Lorsban Advanced',      NULL),
    ('BIOT',   'BioTrek',               NULL),
    ('JUMPST', 'Jumpstart',             'Rhizobium inoculant'),
    ('EXCAL',  'Excalibur',             'Inoculant'),
    ('OPTIMIZE','Optimize',             'Soybean inoculant'),
    ('XTREME', 'XtremeSoy',             NULL),

    -- Adjuvants
    ('MSO',    'Methylated Seed Oil',   'Crop oil concentrate'),
    ('COC',    'Crop Oil Concentrate',  NULL),
    ('NIS',    'Non-Ionic Surfactant',  NULL),
    ('AMS28',  'AMS + UAN 28',          'Tank mix shorthand'),
    ('SURF',   'Surfactant',            NULL),
    ('SILW',   'Silwet L-77',           'Organosilicone surfactant'),
    ('INDUCE', 'Induce',                'Surfactant product'),
    ('PINNACLE','Pinnacle',             'Thifensulfuron adjuvant system')
;

-- ── Exact mapping ─────────────────────────────────────────────────────────────
-- Known free-text strings that map deterministically to a canonical name.

INSERT INTO product_normalization.exact_mapping
    (raw_text, normalized_name, category, notes)
VALUES
    ('roundup',             'Roundup PowerMAX',     'herbicide',  'lowercase brand'),
    ('round up',            'Roundup PowerMAX',     'herbicide',  'spaced variant'),
    ('round-up',            'Roundup PowerMAX',     'herbicide',  'hyphenated variant'),
    ('gly',                 'Glyphosate',            'herbicide',  'ultra-short abbrev'),
    ('glyphosate',          'Glyphosate',            'herbicide',  'generic active'),
    ('liberty link',        'Liberty',               'herbicide',  'trait reference'),
    ('ll',                  'Liberty',               'herbicide',  'LibertyLink shorthand'),
    ('dicamba',             'Dicamba',               'herbicide',  'generic active'),
    ('2,4-d amine',         '2,4-D Amine',           'herbicide',  NULL),
    ('2 4 d',               '2,4-D',                 'herbicide',  'spaced variant'),
    ('atrazine 90df',       'Atrazine 90DF',         'herbicide',  NULL),
    ('python',              'Python WDG',            'herbicide',  NULL),
    ('python wdg',          'Python WDG',            'herbicide',  NULL),
    ('warrant',             'Warrant',               'herbicide',  'Acetochlor'),
    ('warrant ultra',       'Warrant Ultra',         'herbicide',  NULL),
    ('prefix',              'Prefix',                'herbicide',  NULL),
    ('flexstar',            'Flexstar',              'herbicide',  NULL),
    ('flexstar gt',         'Flexstar GT',           'herbicide',  NULL),
    ('status',              'Status',                'herbicide',  NULL),
    ('clarity',             'Clarity',               'herbicide',  NULL),
    ('engenia',             'Engenia',               'herbicide',  NULL),
    ('xtendimax',           'Xtendimax with VaporGrip', 'herbicide', NULL),
    ('tavium',              'Tavium',                'herbicide',  NULL),
    ('diflexx',             'DiFlexx',               'herbicide',  NULL),
    ('halex gt',            'Halex GT',              'herbicide',  NULL),
    ('lexar ez',            'Lexar EZ',              'herbicide',  NULL),
    ('lumax ez',            'Lumax EZ',              'herbicide',  NULL),
    ('fierce',              'Fierce',                'herbicide',  NULL),
    ('valor',               'Valor',                 'herbicide',  NULL),
    ('valor xl',            'Valor XL',              'herbicide',  NULL),

    -- Fungicides
    ('headline',            'Headline',              'fungicide',  NULL),
    ('headline amp',        'Headline AMP',          'fungicide',  NULL),
    ('priaxor',             'Priaxor',               'fungicide',  NULL),
    ('proline',             'Proline',               'fungicide',  NULL),
    ('quilt xcel',          'Quilt Xcel',            'fungicide',  NULL),
    ('quilt',               'Quilt Xcel',            'fungicide',  'shortened brand'),
    ('aproach prima',       'Aproach Prima',         'fungicide',  NULL),
    ('miravis neo',         'Miravis Neo',           'fungicide',  NULL),
    ('delaro complete',     'Delaro Complete',       'fungicide',  NULL),
    ('trivapro',            'Trivapro',              'fungicide',  NULL),
    ('stratego yl',         'Stratego YLD',          'fungicide',  'typo variant'),
    ('stratego yld',        'Stratego YLD',          'fungicide',  NULL),
    ('tilt',                'Tilt',                  'fungicide',  NULL),
    ('caramba',             'Caramba',               'fungicide',  NULL),

    -- Insecticides
    ('warrior ii',          'Warrior II',            'insecticide', NULL),
    ('warrior 2',           'Warrior II',            'insecticide', 'numeral variant'),
    ('karate z',            'Karate Z',              'insecticide', NULL),
    ('baythroid xl',        'Baythroid XL',          'insecticide', NULL),
    ('steward',             'Steward',               'insecticide', NULL),
    ('belt',                'Belt',                  'insecticide', NULL),
    ('lannate',             'Lannate LV',            'insecticide', NULL),
    ('lannate lv',          'Lannate LV',            'insecticide', NULL),
    ('dimilin',             'Dimilin',               'insecticide', NULL),
    ('assail 30sg',         'Assail 30SG',           'insecticide', NULL),
    ('assail',              'Assail 30SG',           'insecticide', 'short form'),

    -- Fertilizers
    ('28-0-0',              'UAN 28-0-0',            'fertilizer', NULL),
    ('32-0-0',              'UAN 32-0-0',            'fertilizer', NULL),
    ('10-34-0',             'Liquid Phosphate 10-34-0', 'fertilizer', NULL),
    ('11-52-0',             'MAP 11-52-0',           'fertilizer', NULL),
    ('18-46-0',             'DAP 18-46-0',           'fertilizer', NULL),
    ('0-0-60',              'Potash 0-0-60',         'fertilizer', NULL),
    ('46-0-0',              'Urea 46-0-0',           'fertilizer', NULL),
    ('21-0-0-24',           'Ammonium Sulfate 21-0-0-24S', 'fertilizer', NULL),
    ('liquid nitrogen',     'UAN 28-0-0',            'fertilizer', 'generic descriptor'),

    -- Adjuvants
    ('crop oil',            'Crop Oil Concentrate',  'adjuvant',   NULL),
    ('crop oil concentrate','Crop Oil Concentrate',  'adjuvant',   NULL),
    ('non-ionic surfactant','Non-Ionic Surfactant',  'adjuvant',   NULL),
    ('nis',                 'Non-Ionic Surfactant',  'adjuvant',   NULL),
    ('mso',                 'Methylated Seed Oil',   'adjuvant',   NULL),
    ('silwet',              'Silwet L-77',           'adjuvant',   NULL),
    ('induce',              'Induce',                'adjuvant',   NULL)
;

-- ── Custom rules ──────────────────────────────────────────────────────────────
-- Regex patterns evaluated after abbreviation expansion and NPK detection.

INSERT INTO product_normalization.custom_rules
    (pattern, normalized_name, category, priority, notes)
VALUES
    -- Rate-embedded strings  e.g. "Roundup 22oz", "Atrazine 1qt"
    ('(?i)^roundup\s+[\d\.]+\s*(oz|qt|pt|gal|fl oz)?$',
     'Roundup PowerMAX', 'herbicide', 10, 'Rate-embedded roundup'),

    ('(?i)^glyphosate\s+[\d\.]+\s*(oz|qt|pt|gal|fl oz|lb)?$',
     'Glyphosate', 'herbicide', 10, 'Rate-embedded glyphosate'),

    ('(?i)^atrazine\s+[\d\.]+\s*(oz|qt|pt|gal|fl oz|lb)?$',
     'Atrazine', 'herbicide', 10, 'Rate-embedded atrazine'),

    ('(?i)^liberty\s+[\d\.]+\s*(oz|qt|pt|gal|fl oz)?$',
     'Liberty', 'herbicide', 10, 'Rate-embedded liberty'),

    -- Liquid nitrogen / UAN variants
    ('(?i)(liquid\s+n|l\.?n\.?|uan[\s\-]?(28|32))',
     'UAN 28-0-0', 'fertilizer', 20, 'Liquid nitrogen variants'),

    -- Potash variants
    ('(?i)(mop|muriate|0[\-\s]0[\-\s]6[02])',
     'Potash 0-0-60', 'fertilizer', 20, 'Potash variants'),

    -- Inoculant catch-all
    ('(?i)(inoculant|rhizobium|bradyrhi)',
     'Soybean Inoculant', 'biological', 30, 'Inoculant catch-all'),

    -- Water / carrier — junk
    ('(?i)^\s*(water|carrier|h2o|rinse)\s*$',
     'CARRIER', 'adjuvant', 5, 'Water/carrier entries'),

    -- Stacked trait references only (no product)
    ('(?i)^(rrx2|rr2|gt27|enlist|ll|xtend|conv(inia)?)\s*$',
     'TRAIT_REFERENCE', 'other', 5, 'GMO trait reference only — not a product'),

    -- Generic fungicide spray entries
    ('(?i)^fung(icide)?\s*$',
     'Fungicide (unspecified)', 'fungicide', 50, 'Generic fungicide entry')
;
