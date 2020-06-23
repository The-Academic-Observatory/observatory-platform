comptext_larger = [
    'substantialy lower than',
    'somewhat lower than',
    'approximately equal to',
    'somewhat larger than',
    'substantially larger than'
]

comptext_bigness = [
    'a very small',
    'a smaller than average',
    'an average',
    'a larger than average',
    'a very large'
]

comptext_quartiles = [
    'fourth quartile',
    'third quartile',
    'second quartile',
    'first quartile'
]

comptext_higherlower = [
    'lower than',
    'roughly the same as',
    'higher than'
]

#TODO Tidy this up # General BQ Defaults
project_id = 'academic-observatory-sandbox'
scope = ''
funder_scope = ''
institutions_scope = """and
  institutions.id in (SELECT id FROM `open-knowledge-publications.institutional_oa_evaluation_2020.grids_in_scope`)"""

print("y")