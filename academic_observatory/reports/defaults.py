import seaborn as sns


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


#List of output types for research publications
output_types = [
    'Journal Articles',
    'Proceedings',
    'Books',
    'Book Sections',
    'Edited Volumes',
    'Reports‡',
    'Datasets‡'
]


# Create colour palette used in graphics to symbolise output types
husl = sns.color_palette(n_colors=len(output_types))
outputs_palette = dict([(output_type, husl[i])
                        for i, output_type in enumerate(output_types)])
outputs_palette.update({'Total Outputs': 'black'})