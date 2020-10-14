import seaborn as sns

project_id = None
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
    'proceedings_article': 'Proceedings',
    'authored_books': 'Books',
    'book_sections': 'Book Sections',
    'edited_volumes': 'Edited Volumes',
    'reports': 'Reports',
    'datasets': 'Datasets',
    'other_outputs': 'Other'
}}

# List of output types for research publications
output_types = [
    'Journal Articles',
    'Proceedings',
    'Books',
    'Book Sections',
    'Edited Volumes',
    'Reports',
    'Datasets'
]
# Discipline lists
# MAG Level0 disciplines

disciplines_mag_level0 = [
    'Medicine',
    'Biology',
    'Environmental science',
    'Chemistry',
    'Materials science',
    'Geology',
    'Physics',
    'Computer science',
    'Engineering',
    'Mathematics',
    'Geography',
    'Psychology',
    'Economics',
    'Business',
    'Political science',
    'Sociology',
    'History',
    'Philosophy',
    'Art'
]

# List of types of Open Access
oa_types = [
    'Open Access (%)',
    'Total Gold OA (%)',
    'Total Green OA (%)',
    'Hybrid OA (%)',
    # 'Green in IR (%)',
]

# Types of Funder (Fundref Data)

funder_types = [
    'National government',
    'Research institutes and centers',
    'Federal/National Government',
    'null',
    'Trusts, charities, foundations (both publically funded and privately funded)',
    'Local government',
    'Universities (academic only)',
    'Associations and societies (private and public)',
    'international',
    'For-profit companies (industry)',
    'other non-profit',
    'government non-federal',
    'Other non-profit',
    'foundation',
    'professional associations and societies',
    'federal',
    'corporate',
    'academic'
]

# Standardisation of funder types

funder_type_clean = {'funder_type': {
    'National government': 'National Government',
    'Research institutes and centers': 'Research Institute',
    'Federal/National Government': 'National Government',
    'null': 'None',
    'Trusts, charities, foundations (both publically funded and privately funded)': 'Foundation',
    'Local government': 'Non-national Government',
    'Universities (academic only)': 'Academic Institution',
    'Associations and societies (private and public)': 'Association',
    'international': 'International',
    'For-profit companies (industry)': 'Corporate',
    'other non-profit': 'Other non-profit',
    'government non-federal': 'Non-national Government',
    'Other non-profit': 'Other non-profit',
    'foundation': 'Foundation',
    'professional associations and societies': 'Association',
    'federal': 'National Government',
    'corporate': 'Corporate',
    'academic': 'Academic Institution'}
}

clean_funder_types = list(set(funder_type_clean['funder_type'].values()))

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

# Disciplines Palettes
colors = sns.color_palette('Set3') * 2
mag_level0_palette = {field: colors[i] for i, field in enumerate(disciplines_mag_level0)}
