import seaborn as sns

#TODO Tidy this up # General BQ Defaults
project_id = 'academic-observatory-sandbox'
scope = ''
funder_scope = ''
institutions_scope = """and
  institutions.id in (SELECT id FROM `open-knowledge-publications.institutional_oa_evaluation_2020.grids_in_scope`)"""


# --- Text expansion defaults ---

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


# --- Name Standardisation ---

# Standard Name Changes
country_clean = {"country": {
    "United Kingdom of Great Britain and Northern Ireland":
    "United Kingdom",
        "Iran (Islamic Republic of)": "Iran",
        "Korea, Republic of": "South Korea",
        "Taiwan, Province of China": "Taiwan"
}}

# Standardisation of the names of output types
outputs_clean = {'type': {
    'total': 'Total Outputs',
    'journal_articles': 'Journal Articles',
    'proceedings_articles': 'Proceedings',
    'authored_books': 'Books',
    'book_sections': 'Book Sections',
                     'edited_volumes': 'Edited Volumes',
                     'reports': 'Reports‡',
                     'datasets': 'Datasets‡'
}}


# List of output types for research publications
output_types = [
    'Journal Articles',
    'Proceedings',
    'Books',
    'Book Sections',
    'Edited Volumes',
    'Reports‡',
    'Datasets‡'
]

# List of types of Open Access
oa_types = [
    'Open Access (%)',
    'Total Gold OA (%)',
    'Total Green OA (%)',
    'Hybrid OA (%)',
    # 'Green in IR (%)',
]


# --- Palettes ---

# Colour palette used in graphics to symbolise global regions
region_palette = {
    'Asia': 'orange',
    'Europe': 'limegreen',
    'North America': 'dodgerblue',
    'Latin America': 'brown',
    'Americas': 'dodgerblue',
    'Africa': 'magenta',
    'Oceania': 'red'
}

# Colour palette used in graphics to symbolise Open Access types
oatypes_palette = {
    'Open Access (%)': 'black',
    'Total Gold OA (%)': 'gold',
    'Total Green OA (%)': 'darkgreen',
    'Hybrid OA (%)': 'orange',
    'Bronze (%)': 'brown',
    'Green in IR (%)': 'limegreen'
}

# Create colour palette used in graphics to symbolise output types
husl = sns.color_palette(n_colors=len(output_types))
outputs_palette = dict([(output_type, husl[i])
                        for i, output_type in enumerate(output_types)])
outputs_palette.update({'Total Outputs': 'black'})